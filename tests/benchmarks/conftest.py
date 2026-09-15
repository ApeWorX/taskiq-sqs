from collections.abc import AsyncGenerator

import pytest
from taskiq.acks import AckableMessage
from taskiq.result import TaskiqResult

from tests.conftest import AWSCredentials, _queue_name_from_url

from taskiq_sqs import S3ResultBackend, SQSBroker
from taskiq_sqs.types import SQSQueue


BATCH_SIZE = 10
RESULT_TASK_ID = "benchmark-task"


async def ack(message: AckableMessage) -> None:
    result = message.ack()
    if result is not None:
        await result


@pytest.fixture
async def bench_broker(
    aws_credentials: AWSCredentials,
    sqs_queue: str,
) -> AsyncGenerator[SQSBroker, None]:
    """The shared `sqs_broker`, but tuned for measuring a round-trip.

    Two settings differ from the default broker: long polling, because with `wait_time_seconds=0` a receive returns
    immediately and usually empty, so a round-trip would measure idle polls; and a batch size that lets a single
    `ReceiveMessage` call serve the whole batch.
    """
    broker = SQSBroker(
        queues=SQSQueue(
            name=_queue_name_from_url(sqs_queue),
            wait_time_seconds=1,
            max_number_of_messages=BATCH_SIZE,
        ),
        **aws_credentials,
    )
    await broker.startup()
    yield broker
    await broker.shutdown()


@pytest.fixture
async def listener(bench_broker: SQSBroker) -> AsyncGenerator[AsyncGenerator[AckableMessage, None], None]:
    """A single `listen()` generator, created outside the measured test body."""
    messages = bench_broker.listen()
    yield messages
    await messages.aclose()


@pytest.fixture
async def stored_result(s3_backend: S3ResultBackend[str]) -> TaskiqResult[str]:
    """A result already written to the bucket, so read benchmarks only measure reads."""
    result: TaskiqResult[str] = TaskiqResult(
        is_err=False,
        return_value="benchmark",
        execution_time=0.1,
        log=None,
    )
    await s3_backend.set_result(RESULT_TASK_ID, result)
    return result
