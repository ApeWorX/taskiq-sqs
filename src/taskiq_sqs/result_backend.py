from typing import TYPE_CHECKING, Any, TypeVar

from aiobotocore.session import get_session
from botocore.exceptions import ClientError
from taskiq import AsyncResultBackend
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.compat import model_dump, model_validate
from taskiq.result import TaskiqResult
from taskiq.serializers import JSONSerializer

from taskiq_sqs import constants, exceptions
from taskiq_sqs.types import S3Bucket


if TYPE_CHECKING:
    from types_aiobotocore_s3.client import S3Client

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
        self._session = get_session()
        self._serializer = serializer or JSONSerializer()

    async def _get_client(self) -> "S3Client":
        """
        Retrieves the S3 client, creating it if necessary.

        Returns:
            S3Client: The initialized S3 client.
        """
        self._client_context_creator = self._session.create_client(
            "s3",
            region_name=self._aws_region,
            endpoint_url=self._aws_endpoint_url,
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
        )
        return await self._client_context_creator.__aenter__()

    async def startup(self) -> None:
        """Initialize the result backend."""
        self._s3_client = await self._get_client()
        try:
            await self._ensure_bucket_exists()
        except Exception:
            await self._client_context_creator.__aexit__(None, None, None)
            raise
        return await super().startup()

    async def _ensure_bucket_exists(self) -> None:
        try:
            await self._s3_client.head_bucket(Bucket=self._bucket["name"])
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code not in ("404", "NoSuchBucket"):
                raise exceptions.ResultBackendError(code=code) from exc
            if not self._bucket.get("declare", True):
                raise exceptions.BucketNotFoundError(bucket_name=self._bucket["name"]) from exc
            await self._create_bucket()

    async def _create_bucket(self) -> None:
        create_kwargs: dict[str, Any] = {"Bucket": self._bucket["name"]}
        if self._aws_region and self._aws_region != constants.AWS_DEFAULT_REGION:
            create_kwargs["CreateBucketConfiguration"] = {"LocationConstraint": self._aws_region}
        try:
            await self._s3_client.create_bucket(**create_kwargs)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") != "BucketAlreadyOwnedByYou":  # can be raise between workers
                raise

    async def shutdown(self) -> None:
        """Shut down the result backend."""
        await self._client_context_creator.__aexit__(None, None, None)
        return await super().shutdown()

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
        if self._base_path:
            task_id = f"{self._base_path.rstrip('/')}/{task_id}"

        await self._s3_client.put_object(
            Bucket=self._bucket["name"],
            Key=task_id,
            Body=self._serializer.dumpb(model_dump(result)),
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
        result = None
        if self._base_path:
            task_id = f"{self._base_path.rstrip('/')}/{task_id}"
        try:
            if response := await self._s3_client.get_object(
                Bucket=self._bucket["name"],
                Key=task_id,
            ):
                async with response["Body"] as stream:
                    result = await stream.read()
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in ["NoSuchKey", "404"]:
                raise exceptions.ResultIsMissingError(task_id=task_id) from exc
            raise exceptions.ResultBackendError(code=code) from exc
        if result is None:
            raise exceptions.ResultIsMissingError(task_id=task_id)

        taskiq_result = model_validate(
            TaskiqResult[_ReturnType],
            self._serializer.loadb(result),
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
        if self._base_path:
            task_id = f"{self._base_path.rstrip('/')}/{task_id}"
        try:
            if await self._s3_client.head_object(Bucket=self._bucket["name"], Key=task_id):
                return True
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in ["NoSuchKey", "404"]:
                pass
            else:
                raise exceptions.ResultBackendError(
                    code=code,
                ) from exc
        return False
