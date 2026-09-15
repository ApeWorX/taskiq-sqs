import capo_s3
import pytest
from taskiq.message import TaskiqMessage

from tests.conftest import TEST_OFFLOAD_BUCKET, AWSCredentials

from taskiq_sqs import S3OffloadMiddleware
from taskiq_sqs.exceptions import BucketNotFoundError, OffloadedPayloadMissingError
from taskiq_sqs.middleware import OFFLOAD_KEY_LABEL
from taskiq_sqs.types import S3Bucket


def _message(**kwargs: object) -> TaskiqMessage:
    return TaskiqMessage(
        task_id="test_task_id",
        task_name="test_task",
        labels={},
        args=[],
        kwargs=kwargs,
    )


class TestS3OffloadMiddleware:
    async def test_when_message_is_small__then_it_is_not_offloaded(
        self,
        s3_offload_middleware: S3OffloadMiddleware,
    ) -> None:
        message = _message(value="small")

        result = await s3_offload_middleware.pre_send(message)

        assert result.kwargs == {"value": "small"}
        assert OFFLOAD_KEY_LABEL not in result.labels

    async def test_when_message_is_large__then_it_is_offloaded_to_s3(
        self,
        s3_offload_middleware: S3OffloadMiddleware,
    ) -> None:
        message = _message(value="x" * 1000)

        result = await s3_offload_middleware.pre_send(message)

        assert result.args == []
        assert result.kwargs == {}
        assert OFFLOAD_KEY_LABEL in result.labels

        async with s3_offload_middleware._s3_client.get_object(
            bucket=TEST_OFFLOAD_BUCKET,
            key=result.labels[OFFLOAD_KEY_LABEL],
        ) as output:
            body = b"".join([chunk async for chunk in output["body"]])
        assert b"x" * 1000 in body

    async def test_when_offloaded_message_is_received__then_payload_is_restored(
        self,
        s3_offload_middleware: S3OffloadMiddleware,
    ) -> None:
        original = _message(value="y" * 1000)
        offloaded = await s3_offload_middleware.pre_send(original)

        restored = await s3_offload_middleware.pre_execute(offloaded)

        assert restored.kwargs == {"value": "y" * 1000}

    async def test_when_message_is_not_offloaded__then_pre_execute_is_noop(
        self,
        s3_offload_middleware: S3OffloadMiddleware,
    ) -> None:
        message = _message(value="small")

        result = await s3_offload_middleware.pre_execute(message)

        assert result.kwargs == {"value": "small"}

    async def test_when_offloaded_payload_is_missing__then_pre_execute_raises(
        self,
        s3_offload_middleware: S3OffloadMiddleware,
    ) -> None:
        message = _message()
        message.labels[OFFLOAD_KEY_LABEL] = "nonexistent-key.json"

        with pytest.raises(OffloadedPayloadMissingError):
            await s3_offload_middleware.pre_execute(message)

    async def test_when_task_is_executed__then_offloaded_payload_is_deleted(
        self,
        s3_offload_middleware: S3OffloadMiddleware,
    ) -> None:
        original = _message(value="z" * 1000)
        offloaded = await s3_offload_middleware.pre_send(original)
        key = offloaded.labels[OFFLOAD_KEY_LABEL]

        await s3_offload_middleware.post_execute(offloaded, result=None)

        with pytest.raises(capo_s3.errors.NoSuchKey):
            async with s3_offload_middleware._s3_client.get_object(bucket=TEST_OFFLOAD_BUCKET, key=key):
                pass

    async def test_when_delete_after_execute_is_false__then_payload_is_kept(
        self,
        aws_credentials: AWSCredentials,
        s3_offload_bucket: str,  # noqa: ARG002
    ) -> None:
        middleware = S3OffloadMiddleware(
            bucket=S3Bucket(name=TEST_OFFLOAD_BUCKET),
            max_message_size=64,
            delete_after_execute=False,
            **aws_credentials,
        )
        await middleware.startup()
        try:
            offloaded = await middleware.pre_send(_message(value="w" * 1000))
            key = offloaded.labels[OFFLOAD_KEY_LABEL]

            await middleware.post_execute(offloaded, result=None)

            async with middleware._s3_client.get_object(bucket=TEST_OFFLOAD_BUCKET, key=key) as output:
                body = b"".join([chunk async for chunk in output["body"]])
            assert b"w" * 1000 in body
        finally:
            await middleware.shutdown()

    async def test_when_bucket_missing_and_declare_false__then_startup_raises(
        self,
        aws_credentials: AWSCredentials,
    ) -> None:
        middleware = S3OffloadMiddleware(
            bucket=S3Bucket(name="nonexistent-offload-bucket", declare=False),
            **aws_credentials,
        )
        with pytest.raises(BucketNotFoundError):
            await middleware.startup()
