import uuid
from collections.abc import AsyncGenerator
from typing import Any, TypedDict

import capo_s3
import capo_sqs
import pytest
from taskiq import BrokerMessage

from taskiq_sqs import S3OffloadMiddleware, S3ResultBackend, SQSBroker
from taskiq_sqs.types import S3Bucket, SQSQueue


ENDPOINT_URL = "http://localhost:4566"
TEST_BUCKET = "test-bucket"
TEST_OFFLOAD_BUCKET = "test-offload-bucket"
QUEUE_NAME = "test-queue"


class AWSCredentials(TypedDict):
    endpoint_url: str
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_region_name: str


@pytest.fixture(scope="session")
def aws_credentials() -> AWSCredentials:
    """Mocked AWS Credentials for ministack."""
    return AWSCredentials(
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id="your-aws-id",
        aws_secret_access_key="your-aws-access-key",  # noqa: S106  # pragma: allowlist secret
        aws_region_name="us-east-1",
    )


@pytest.fixture
async def s3_client(aws_credentials: AWSCredentials) -> AsyncGenerator[capo_s3.AsyncS3Client, Any]:
    """An S3 client independent from the one `S3ResultBackend`/`S3OffloadMiddleware` build internally.

    Used to verify state out of band, so a test doesn't just check that a component agrees with itself.
    """
    client = capo_s3.AsyncS3Client(
        region=aws_credentials["aws_region_name"],
        endpoint=aws_credentials["endpoint_url"],
        credentials=capo_s3.Credentials(
            access_key=aws_credentials["aws_access_key_id"],
            secret_key=aws_credentials["aws_secret_access_key"],
        ),
        force_path_style=True,
    )
    await client.__aenter__()
    yield client
    await client.__aexit__(None, None, None)


@pytest.fixture
async def s3_bucket(s3_client: capo_s3.AsyncS3Client) -> AsyncGenerator[str, Any]:
    await s3_client.create_bucket(bucket=TEST_BUCKET)
    yield TEST_BUCKET
    await _empty_bucket(s3_client, TEST_BUCKET)
    await s3_client.delete_bucket(bucket=TEST_BUCKET)


async def _empty_bucket(s3_client: capo_s3.AsyncS3Client, bucket: str) -> None:
    response = await s3_client.list_objects_v2(bucket=bucket)
    objects = [{"key": obj["key"]} for obj in response.get("contents", []) if "key" in obj]
    if objects:
        await s3_client.delete_objects(bucket=bucket, delete={"objects": objects})


@pytest.fixture
async def s3_backend(
    aws_credentials: AWSCredentials,
    s3_bucket: str,  # noqa: ARG001
) -> AsyncGenerator[S3ResultBackend, Any]:
    backend = S3ResultBackend(bucket=S3Bucket(name=TEST_BUCKET), **aws_credentials)
    await backend.startup()
    assert backend._s3_client
    yield backend
    await backend.shutdown()


@pytest.fixture
async def s3_offload_bucket(s3_client: capo_s3.AsyncS3Client) -> AsyncGenerator[str, Any]:
    await s3_client.create_bucket(bucket=TEST_OFFLOAD_BUCKET)
    yield TEST_OFFLOAD_BUCKET
    await _empty_bucket(s3_client, TEST_OFFLOAD_BUCKET)
    await s3_client.delete_bucket(bucket=TEST_OFFLOAD_BUCKET)


@pytest.fixture
async def s3_offload_middleware(
    aws_credentials: AWSCredentials,
    s3_offload_bucket: str,  # noqa: ARG001
) -> AsyncGenerator[S3OffloadMiddleware, Any]:
    middleware = S3OffloadMiddleware(
        bucket=S3Bucket(name=TEST_OFFLOAD_BUCKET),
        max_message_size=64,
        **aws_credentials,
    )
    await middleware.startup()
    yield middleware
    await middleware.shutdown()


@pytest.fixture
async def sqs_client(aws_credentials: AWSCredentials) -> AsyncGenerator[capo_sqs.AsyncSQSClient, Any]:
    """An SQS client independent from the one `SQSBroker` builds internally, for out-of-band verification."""
    client = capo_sqs.AsyncSQSClient(
        region=aws_credentials["aws_region_name"],
        endpoint=aws_credentials["endpoint_url"],
        credentials=capo_sqs.Credentials(
            access_key=aws_credentials["aws_access_key_id"],
            secret_key=aws_credentials["aws_secret_access_key"],
        ),
    )
    await client.__aenter__()
    yield client
    await client.__aexit__(None, None, None)


async def _create_queue(sqs_client: capo_sqs.AsyncSQSClient, name: str) -> str:
    response = await sqs_client.create_queue(queue_name=name)
    queue_url = response.get("queue_url")
    assert queue_url is not None
    return queue_url


@pytest.fixture
async def sqs_queue(sqs_client: capo_sqs.AsyncSQSClient) -> AsyncGenerator[str, Any]:
    queue_url = await _create_queue(sqs_client, f"{QUEUE_NAME}-{uuid.uuid4().hex}")
    yield queue_url
    await sqs_client.delete_queue(queue_url=queue_url)


@pytest.fixture
async def sqs_second_queue(sqs_client: capo_sqs.AsyncSQSClient) -> AsyncGenerator[str, Any]:
    queue_url = await _create_queue(sqs_client, f"{QUEUE_NAME}-second-{uuid.uuid4().hex}")
    yield queue_url
    await sqs_client.delete_queue(queue_url=queue_url)


def _queue_name_from_url(queue_url: str) -> str:
    return queue_url.rsplit("/", maxsplit=1)[-1]


@pytest.fixture
async def sqs_broker(
    aws_credentials: AWSCredentials,
    sqs_queue: str,
) -> AsyncGenerator[SQSBroker, Any]:
    broker = SQSBroker(
        queues=SQSQueue(name=_queue_name_from_url(sqs_queue)),
        **aws_credentials,
    )
    await broker.startup()
    assert broker._sqs_client
    assert broker._queue_urls
    yield broker
    await broker.shutdown()


@pytest.fixture
async def multiqueue_sqs_broker(
    aws_credentials: AWSCredentials,
    sqs_queue: str,
    sqs_second_queue: str,
) -> AsyncGenerator[SQSBroker, Any]:
    broker = SQSBroker(
        queues=[
            SQSQueue(name=_queue_name_from_url(sqs_queue)),
            SQSQueue(name=_queue_name_from_url(sqs_second_queue)),
        ],
        **aws_credentials,
    )
    await broker.startup()
    yield broker
    await broker.shutdown()


@pytest.fixture
def broker_message() -> BrokerMessage:
    return BrokerMessage(
        task_id="test_task",
        task_name="test_task",
        message=b"test_message",
        labels={},
    )
