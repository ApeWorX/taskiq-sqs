import contextlib
import logging
from typing import Any

import capo_s3
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.message import TaskiqMessage
from taskiq.serializers import JSONSerializer

from taskiq_sqs import constants, exceptions
from taskiq_sqs.types import S3Bucket


logger = logging.getLogger(__name__)

OFFLOAD_KEY_LABEL = "s3_offload_key"


class S3OffloadMiddleware(TaskiqMiddleware):
    """
    Offloads large task payloads to S3 instead of sending them through SQS.

    SQS messages are limited to 256 KiB, so payloads that would exceed a configured threshold are uploaded to S3 before
    the message is sent, and the message itself only carries a reference (bucket key) to the uploaded payload.
    On the worker side, the original arguments are downloaded back from S3 before task execution.
    """

    def __init__(
        self,
        bucket: S3Bucket,
        max_message_size: int = constants.DEFAULT_S3_OFFLOAD_THRESHOLD_BYTES,
        base_path: str = "",
        endpoint_url: str | None = None,
        aws_region_name: str = constants.AWS_DEFAULT_REGION,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        delete_after_execute: bool = True,
        serializer: TaskiqSerializer | None = None,
    ) -> None:
        """
        Constructs a new S3 offload middleware.

        :param bucket: S3 bucket configuration.
        :param max_message_size: payloads larger than this many bytes are offloaded to S3.
        :param base_path: base path (prefix) for offloaded payloads.
        :param endpoint_url: endpoint URL for S3.
        :param aws_region_name: AWS region, default is 'us-east-1'.
        :param aws_access_key_id: AWS access key ID.
        :param aws_secret_access_key: AWS secret access key.
        :param delete_after_execute: whether to delete the offloaded payload from S3 after the task has been executed
            by the worker.
        :param serializer: serializer used to encode/decode offloaded payloads.
        """
        super().__init__()
        self._bucket = bucket
        self._max_message_size = max_message_size
        self._base_path = base_path
        self._aws_endpoint_url = endpoint_url
        self._aws_region = aws_region_name
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._delete_after_execute = delete_after_execute
        self._serializer = serializer or JSONSerializer()

    async def startup(self) -> None:
        """Initialize the S3 client and ensure the bucket exists."""
        credentials = None
        if self._aws_access_key_id and self._aws_secret_access_key:
            credentials = capo_s3.Credentials(
                access_key=self._aws_access_key_id,
                secret_key=self._aws_secret_access_key,
            )
        self._s3_client = capo_s3.AsyncS3Client(
            region=self._aws_region,
            endpoint=self._aws_endpoint_url,
            credentials=credentials,
            force_path_style=True,
        )
        await self._s3_client.__aenter__()
        try:
            await self._ensure_bucket_exists()
        except Exception:
            await self._s3_client.__aexit__(None, None, None)
            raise

    async def shutdown(self) -> None:
        """Shut down the S3 client."""
        await self._s3_client.__aexit__(None, None, None)

    async def _ensure_bucket_exists(self) -> None:
        try:
            await self._s3_client.head_bucket(bucket=self._bucket["name"])
        except capo_s3.errors.NotFound:
            if not self._bucket.get("is_declare", True):
                raise exceptions.BucketNotFoundError(bucket_name=self._bucket["name"]) from None
            await self._create_bucket()

    async def _create_bucket(self) -> None:
        create_kwargs: dict[str, Any] = dict(self._bucket.get("options", {}))
        if self._aws_region and self._aws_region != constants.AWS_DEFAULT_REGION:
            create_kwargs["create_bucket_configuration"] = {"location_constraint": self._aws_region}
        with contextlib.suppress(capo_s3.errors.BucketAlreadyOwnedByYou):
            await self._s3_client.create_bucket(bucket=self._bucket["name"], **create_kwargs)

    def _build_key(self, task_id: str) -> str:
        key = f"{task_id}.json"
        if self._base_path:
            key = f"{self._base_path.rstrip('/')}/{key}"
        return key

    async def pre_send(self, message: TaskiqMessage) -> TaskiqMessage:
        """
        Offload the message payload to S3 if it's too large to send through SQS.

        :param message: message to send.
        :return: message with args/kwargs replaced by an S3 reference, if offloaded.
        """
        payload = self._serializer.dumpb({"args": message.args, "kwargs": message.kwargs})
        if len(payload) <= self._max_message_size:
            return message

        key = self._build_key(message.task_id)
        await self._s3_client.put_object(bucket=self._bucket["name"], key=key, body=payload)
        logger.debug("Offloaded payload of task '%s' to s3://%s/%s", message.task_id, self._bucket["name"], key)

        message.args = []
        message.kwargs = {}
        message.labels[OFFLOAD_KEY_LABEL] = key
        return message

    async def pre_execute(self, message: TaskiqMessage) -> TaskiqMessage:
        """
        Restore the original message payload from S3, if it was offloaded.

        :param message: incoming parsed taskiq message.
        :return: message with the original args/kwargs restored.
        """
        key = message.labels.get(OFFLOAD_KEY_LABEL)
        if key is None:
            return message

        try:
            async with self._s3_client.get_object(bucket=self._bucket["name"], key=key) as output:
                body = b"".join([chunk async for chunk in output["body"]])
        except capo_s3.errors.NoSuchKey as exc:
            raise exceptions.OffloadedPayloadMissingError(
                task_id=message.task_id,
                bucket_name=self._bucket["name"],
                key=key,
            ) from exc

        payload = self._serializer.loadb(body)
        message.args = payload["args"]
        message.kwargs = payload["kwargs"]
        return message

    async def post_execute(self, message: TaskiqMessage, result: Any) -> None:  # noqa: ARG002
        """
        Delete the offloaded payload from S3 once the task has been executed.

        :param message: processed message.
        :param result: result of execution for current task, not used.
        """
        key = message.labels.pop(OFFLOAD_KEY_LABEL, None)
        if key is None or not self._delete_after_execute:
            return
        await self._s3_client.delete_object(bucket=self._bucket["name"], key=key)
