import uuid
from collections.abc import AsyncGenerator
from typing import Any

import capo_s3
import pytest
from taskiq.result import TaskiqResult

from tests.conftest import AWSCredentials

from taskiq_sqs import S3ResultBackend
from taskiq_sqs.exceptions import BucketNotFoundError, ResultIsMissingError
from taskiq_sqs.types import S3Bucket


@pytest.fixture
def taskiq_result() -> TaskiqResult:
    return TaskiqResult(return_value="test_value", is_err=True, execution_time=0.1)


class TestResultBackend:
    async def test_when_result_set__then_result_is_actually_saved_to_s3(
        self,
        s3_backend: S3ResultBackend,
        s3_client: capo_s3.AsyncS3Client,
        s3_bucket: str,
        taskiq_result: TaskiqResult,
    ) -> None:
        await s3_backend.set_result("test_task_id", taskiq_result)

        async with s3_client.get_object(bucket=s3_bucket, key="test_task_id") as response:
            assert response["body"] is not None

    async def test_when_result_present_in_s3__then_get_result_return_it(
        self,
        s3_backend: S3ResultBackend,
        taskiq_result: TaskiqResult,
    ) -> None:
        await s3_backend.set_result("test_task_id", taskiq_result)

        retrieved_result = await s3_backend.get_result("test_task_id")
        assert retrieved_result.return_value == "test_value"
        assert retrieved_result.is_err is True

    async def test_when_set_result_is_called__then_save_it_to_right_path(
        self,
        s3_backend: S3ResultBackend,
        s3_client: capo_s3.AsyncS3Client,
        s3_bucket: str,
        taskiq_result: TaskiqResult,
    ) -> None:
        s3_backend._base_path = "results"
        await s3_backend.set_result("test_task_id", taskiq_result)

        response = await s3_client.head_object(bucket=s3_bucket, key="results/test_task_id")
        assert response is not None

    async def test_when_result_is_set__then_we_should_be_able_to_get_it(
        self,
        s3_backend: S3ResultBackend,
        taskiq_result: TaskiqResult,
    ) -> None:
        s3_backend._base_path = "results"
        await s3_backend.set_result("test_task_id", taskiq_result)

        retrieved_result = await s3_backend.get_result("test_task_id")
        assert retrieved_result.return_value == "test_value"
        assert retrieved_result.is_err is True

    async def test_when_result_is_missing__then_get_result_should_raise_an_error(
        self,
        s3_backend: S3ResultBackend,
    ) -> None:
        with pytest.raises(ResultIsMissingError):
            await s3_backend.get_result("non_existent_task_id")

    async def test_when_result_exists__when_is_result_ready_should_return_true(
        self,
        s3_backend: S3ResultBackend,
        taskiq_result: TaskiqResult,
    ) -> None:
        s3_backend._base_path = "results"
        await s3_backend.set_result("test_task_id", taskiq_result)

        assert await s3_backend.is_result_ready("test_task_id") is True
        assert await s3_backend.is_result_ready("non_existent_task_id") is False

    async def test_when_get_result_without_logs__then_we_should_return_none(self, s3_backend: S3ResultBackend) -> None:
        result = TaskiqResult(
            return_value="test_value",
            log="test_log",
            is_err=False,
            execution_time=0.1,
        )
        await s3_backend.set_result("test_task_id", result)

        retrieved_result = await s3_backend.get_result("test_task_id", with_logs=False)
        assert retrieved_result.return_value == "test_value"
        assert retrieved_result.is_err is False
        assert retrieved_result.log is None

    async def test_when_get_result_with_logs__then_we_should_return_them(self, s3_backend: S3ResultBackend) -> None:
        result = TaskiqResult(
            return_value="test_value",
            is_err=True,
            log="test_log",
            execution_time=0.1,
        )
        await s3_backend.set_result("test_task_id", result)

        retrieved_result = await s3_backend.get_result("test_task_id", with_logs=True)
        assert retrieved_result.return_value == "test_value"
        assert retrieved_result.is_err is True
        assert retrieved_result.log == "test_log"


class TestBucketDeclare:
    tmp_bucket_name: str
    backend: S3ResultBackend | None

    @staticmethod
    async def _bucket_exists(s3_client: capo_s3.AsyncS3Client, name: str) -> bool:
        response = await s3_client.list_buckets()
        return any(bucket.get("name") == name for bucket in response.get("buckets", []))

    @pytest.fixture(autouse=True)
    async def _setup(self, s3_client: capo_s3.AsyncS3Client) -> AsyncGenerator[None, Any]:
        self.tmp_bucket_name = f"declare-test-{uuid.uuid4().hex[:8]}"
        self.backend = None
        yield
        if self.backend is not None:
            await self.backend.shutdown()
        if not await self._bucket_exists(s3_client, self.tmp_bucket_name):
            return
        response = await s3_client.list_objects_v2(bucket=self.tmp_bucket_name)
        objects = [{"key": obj["key"]} for obj in response.get("contents", []) if "key" in obj]
        if objects:
            await s3_client.delete_objects(bucket=self.tmp_bucket_name, delete={"objects": objects})
        await s3_client.delete_bucket(bucket=self.tmp_bucket_name)

    async def test_when_declare_true_and_bucket_missing__then_bucket_is_created_on_startup(
        self,
        aws_credentials: AWSCredentials,
        s3_client: capo_s3.AsyncS3Client,
    ) -> None:
        self.backend = S3ResultBackend(
            bucket=S3Bucket(name=self.tmp_bucket_name, is_declare=True),
            **aws_credentials,
        )
        await self.backend.startup()

        assert await self._bucket_exists(s3_client, self.tmp_bucket_name)

    async def test_when_declare_false_and_bucket_missing__then_startup_raises(
        self,
        aws_credentials: AWSCredentials,
        s3_client: capo_s3.AsyncS3Client,
    ) -> None:
        backend = S3ResultBackend(
            bucket=S3Bucket(name=self.tmp_bucket_name, is_declare=False),
            **aws_credentials,
        )

        with pytest.raises(BucketNotFoundError):
            await backend.startup()

        assert not await self._bucket_exists(s3_client, self.tmp_bucket_name)

    async def test_when_declare_false_and_bucket_exists__then_startup_succeeds(
        self,
        aws_credentials: AWSCredentials,
        s3_client: capo_s3.AsyncS3Client,
        s3_bucket: str,
    ) -> None:
        self.backend = S3ResultBackend(
            bucket=S3Bucket(name=s3_bucket, is_declare=False),
            **aws_credentials,
        )
        await self.backend.startup()

        assert await self._bucket_exists(s3_client, s3_bucket)

    async def test_when_declare_true_and_bucket_already_exists__then_startup_is_idempotent(
        self,
        aws_credentials: AWSCredentials,
        s3_client: capo_s3.AsyncS3Client,
        s3_bucket: str,
    ) -> None:
        self.backend = S3ResultBackend(
            bucket=S3Bucket(name=s3_bucket, is_declare=True),
            **aws_credentials,
        )
        await self.backend.startup()

        assert await self._bucket_exists(s3_client, s3_bucket)
