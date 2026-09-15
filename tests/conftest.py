import uuid
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Any, TypedDict

import pytest
from aiobotocore.session import get_session
from taskiq import BrokerMessage
from types_aiobotocore_sqs.client import SQSClient

from taskiq_sqs import S3OffloadMiddleware, S3ResultBackend, SQSBroker
from taskiq_sqs.types import S3Bucket


if TYPE_CHECKING:
    from types_aiobotocore_s3.client import S3Client

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
    """Mocked AWS Credentials for moto."""
    return AWSCredentials(
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id="your-aws-id",
        aws_secret_access_key="your-aws-access-key",  # noqa: S106  # pragma: allowlist secret
        aws_region_name="us-east-1",
    )


@pytest.fixture
async def s3_client(aws_credentials: AWSCredentials) -> "AsyncGenerator[S3Client, Any]":
    client_context = get_session().create_client(
        "s3",
        endpoint_url=aws_credentials["endpoint_url"],
        aws_access_key_id=aws_credentials["aws_access_key_id"],
        aws_secret_access_key=aws_credentials["aws_secret_access_key"],
        region_name=aws_credentials["aws_region_name"],
    )
    yield await client_context.__aenter__()
    await client_context.__aexit__(None, None, None)


@pytest.fixture
async def s3_bucket(s3_client: S3ResultBackend) -> AsyncGenerator[str, Any]:
    response = await s3_client.create_bucket(Bucket=TEST_BUCKET)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    # Ensure the bucket is created
    assert "Location" in response
    assert response["Location"] == f"/{TEST_BUCKET}"
    # Return the bucket name for use in tests
    yield TEST_BUCKET
    # Delete all objects in the bucket
    response = await s3_client.list_objects_v2(Bucket=TEST_BUCKET)
    if "Contents" in response:
        objects_to_delete = [{"Key": obj["Key"]} for obj in response.get("Contents", [])]
        if objects_to_delete:
            await s3_client.delete_objects(
                Bucket=TEST_BUCKET,
                Delete={"Objects": objects_to_delete},
            )

    # Delete the bucket itself
    await s3_client.delete_bucket(Bucket=TEST_BUCKET)


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
async def s3_offload_bucket(s3_client: S3ResultBackend) -> AsyncGenerator[str, Any]:
    response = await s3_client.create_bucket(Bucket=TEST_OFFLOAD_BUCKET)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    yield TEST_OFFLOAD_BUCKET
    response = await s3_client.list_objects_v2(Bucket=TEST_OFFLOAD_BUCKET)
    if "Contents" in response:
        objects_to_delete = [{"Key": obj["Key"]} for obj in response.get("Contents", [])]
        if objects_to_delete:
            await s3_client.delete_objects(
                Bucket=TEST_OFFLOAD_BUCKET,
                Delete={"Objects": objects_to_delete},
            )
    await s3_client.delete_bucket(Bucket=TEST_OFFLOAD_BUCKET)


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
async def sqs_client(aws_credentials: AWSCredentials) -> AsyncGenerator[SQSClient, Any]:
    client_context = get_session().create_client(
        "sqs",
        endpoint_url=aws_credentials["endpoint_url"],
        aws_access_key_id=aws_credentials["aws_access_key_id"],
        aws_secret_access_key=aws_credentials["aws_secret_access_key"],
        region_name=aws_credentials["aws_region_name"],
    )
    yield await client_context.__aenter__()
    await client_context.__aexit__(None, None, None)


@pytest.fixture
async def sqs_queue(sqs_client: SQSClient) -> AsyncGenerator[str, Any]:
    queue_name = f"{QUEUE_NAME}-{uuid.uuid4().hex}"
    response = await sqs_client.create_queue(QueueName=queue_name)
    queue_url = response["QueueUrl"]
    yield queue_url
    await sqs_client.delete_queue(QueueUrl=queue_url)


def _queue_name_from_url(queue_url: str) -> str:
    return queue_url.rsplit("/", maxsplit=1)[-1]


@pytest.fixture
async def sqs_broker(
    aws_credentials: AWSCredentials,
    sqs_queue: str,
) -> AsyncGenerator[SQSBroker, Any]:
    broker = SQSBroker(
        queue_name=_queue_name_from_url(sqs_queue),
        **aws_credentials,
    )
    await broker.startup()
    assert broker._sqs_client
    assert broker._sqs_queue_url
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
