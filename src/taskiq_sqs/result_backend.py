import contextlib
from typing import Any, TypeVar

import capo_s3
from taskiq import AsyncResultBackend
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.compat import model_dump, model_validate
from taskiq.result import TaskiqResult
from taskiq.serializers import JSONSerializer

from taskiq_sqs import constants, exceptions
from taskiq_sqs.types import S3Bucket


_ReturnType = TypeVar("_ReturnType")


class S3ResultBackend(AsyncResultBackend[_ReturnType]):
    """TaskIQ result backend that uses S3."""

    def __init__(
        self,
        bucket: S3Bucket,
        base_path: str = "",
        endpoint_url: str | None = None,
        aws_region_name: str = constants.AWS_DEFAULT_REGION,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        serializer: TaskiqSerializer | None = None,
    ) -> None:
        """
        Constructs a new S3 result backend.

        :param bucket: S3 bucket configuration.
        :param base_path: base path for results.
        :param endpoint_url: endpoint URL for S3.
        :param aws_region_name: AWS region, default is 'us-east-1'.
        :param aws_access_key_id: AWS access key ID.
        :param aws_secret_access_key: AWS secret access key.
        :param serializer: serializer to use.
        """
        self._aws_region = aws_region_name
        self._aws_endpoint_url = endpoint_url
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._bucket = bucket
        self._base_path = base_path
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
        return await super().startup()

    async def _ensure_bucket_exists(self) -> None:
        try:
            await self._s3_client.head_bucket(bucket=self._bucket["name"])
        except capo_s3.errors.NotFound:
            if not self._bucket.get("declare", True):
                raise exceptions.BucketNotFoundError(bucket_name=self._bucket["name"]) from None
            await self._create_bucket()
        except capo_s3.errors.ServiceError as exc:
            raise exceptions.ResultBackendError(code=exc.code) from exc

    async def _create_bucket(self) -> None:
        create_kwargs: dict[str, Any] = {}
        if self._aws_region and self._aws_region != constants.AWS_DEFAULT_REGION:
            create_kwargs["create_bucket_configuration"] = {"location_constraint": self._aws_region}
        with contextlib.suppress(capo_s3.errors.BucketAlreadyOwnedByYou):
            await self._s3_client.create_bucket(bucket=self._bucket["name"], **create_kwargs)

    async def shutdown(self) -> None:
        """Shut down the result backend."""
        await self._s3_client.__aexit__(None, None, None)
        return await super().shutdown()

    def _build_key(self, task_id: str) -> str:
        if self._base_path:
            return f"{self._base_path.rstrip('/')}/{task_id}"
        return task_id

    async def set_result(
        self,
        task_id: str,
        result: TaskiqResult[_ReturnType],
    ) -> None:
        """
        Set result in your backend.

        :param task_id: current task id.
        :param result: result of execution.
        """
        await self._s3_client.put_object(
            bucket=self._bucket["name"],
            key=self._build_key(task_id),
            body=self._serializer.dumpb(model_dump(result)),
        )

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[_ReturnType]:
        """
        Here you must retrieve result by id.

        Logs is a part of a result. Here we have a parameter whether you want to fetch result with logs or not,
        because logs can have a lot of info and sometimes it's critical to get only needed information.

        :param task_id: id of a task.
        :param with_logs: whether to fetch logs.
        :return: result.
        """
        try:
            async with self._s3_client.get_object(bucket=self._bucket["name"], key=self._build_key(task_id)) as output:
                body = b"".join([chunk async for chunk in output["body"]])
        except capo_s3.errors.NoSuchKey as exc:
            raise exceptions.ResultIsMissingError(task_id=task_id) from exc
        except capo_s3.errors.ServiceError as exc:
            raise exceptions.ResultBackendError(code=exc.code) from exc

        taskiq_result = model_validate(
            TaskiqResult[_ReturnType],
            self._serializer.loadb(body),
        )

        if not with_logs:
            taskiq_result.log = None

        return taskiq_result

    async def is_result_ready(self, task_id: str) -> bool:
        """
        Check if result exists.

        :param task_id: id of a task.
        :return: True if result is ready.
        """
        try:
            await self._s3_client.head_object(bucket=self._bucket["name"], key=self._build_key(task_id))
        except capo_s3.errors.NotFound:
            return False
        except capo_s3.errors.ServiceError as exc:
            raise exceptions.ResultBackendError(code=exc.code) from exc
        return True
