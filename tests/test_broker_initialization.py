import asyncio
import uuid

import capo_sqs
import pytest
from taskiq import BrokerMessage

from tests.conftest import AWSCredentials

from taskiq_sqs import SQSBroker
from taskiq_sqs.exceptions import BrokerInitError, QueueNotFoundError
from taskiq_sqs.types import SQSQueue


async def test_when_queue_missing_and_declare_false__then_startup_raises(aws_credentials: AWSCredentials) -> None:
    broker = SQSBroker(
        queues=SQSQueue(name=f"declare-false-{uuid.uuid4().hex}", is_declare=False),
        **aws_credentials,
    )
    with pytest.raises(QueueNotFoundError):
        await broker.startup()


async def test_when_queue_missing_and_declare_true__then_it_is_created(
    aws_credentials: AWSCredentials,
    sqs_client: capo_sqs.AsyncSQSClient,
) -> None:
    queue_name = f"declare-true-{uuid.uuid4().hex}"
    broker = SQSBroker(queues=SQSQueue(name=queue_name), **aws_credentials)  # declare defaults to True
    try:
        await broker.startup()
        response = await sqs_client.get_queue_url(queue_name=queue_name)
        assert response.get("queue_url") == broker._queue_urls[queue_name]
    finally:
        await broker.shutdown()
        await sqs_client.delete_queue(queue_url=broker._queue_urls[queue_name])


async def test_when_queue_declared_with_options__then_they_become_queue_attributes(
    aws_credentials: AWSCredentials,
    sqs_client: capo_sqs.AsyncSQSClient,
) -> None:
    queue_name = f"declare-options-{uuid.uuid4().hex}"
    broker = SQSBroker(
        queues=SQSQueue(name=queue_name, options={"VisibilityTimeout": "1"}),
        **aws_credentials,
    )
    try:
        await broker.startup()
        queue_url = broker._queue_urls[queue_name]
        await sqs_client.send_message(queue_url=queue_url, message_body="hidden")

        generator = broker.listen()
        try:
            await asyncio.wait_for(generator.__anext__(), timeout=2)  # received, deliberately left unacked
        finally:
            await generator.aclose()

        # still within the 1s VisibilityTimeout the queue was declared with, so this must not see it yet
        immediate = await sqs_client.receive_message(queue_url=queue_url)
        assert not immediate.get("messages")
    finally:
        await broker.shutdown()
        await sqs_client.delete_queue(queue_url=broker._queue_urls[queue_name])


async def test_when_fifo_queue_declared__then_fifo_attribute_is_set_automatically(
    aws_credentials: AWSCredentials,
    sqs_client: capo_sqs.AsyncSQSClient,
) -> None:
    queue_name = f"declare-fifo-{uuid.uuid4().hex}.fifo"
    broker = SQSBroker(queues=SQSQueue(name=queue_name), **aws_credentials)
    try:
        await broker.startup()
        message = BrokerMessage(
            task_id="t1",
            task_name="t1",
            message=b"x",
            labels={"group_id": "g1", "deduplication_id": "d1"},
        )
        await broker.kick(message)

        response = await sqs_client.receive_message(queue_url=broker._queue_urls[queue_name])
        assert len(response.get("messages", [])) == 1
    finally:
        await broker.shutdown()
        await sqs_client.delete_queue(queue_url=broker._queue_urls[queue_name])


async def test_max_number_of_messages_error(aws_credentials: AWSCredentials) -> None:
    with pytest.raises(BrokerInitError):
        SQSBroker(
            queues=SQSQueue(name="nonexistent-queue", max_number_of_messages=15),
            **aws_credentials,
        )


async def test_wait_time_seconds_error(aws_credentials: AWSCredentials) -> None:
    with pytest.raises(BrokerInitError):
        SQSBroker(
            queues=SQSQueue(name="nonexistent-queue", wait_time_seconds=21),
            **aws_credentials,
        )


async def test_when_no_queues_given__then_should_raise_an_error(aws_credentials: AWSCredentials) -> None:
    with pytest.raises(BrokerInitError):
        SQSBroker(queues=[], **aws_credentials)


async def test_when_duplicate_queue_names__then_should_raise_an_error(aws_credentials: AWSCredentials) -> None:
    with pytest.raises(BrokerInitError):
        SQSBroker(
            queues=[SQSQueue(name="same-name"), SQSQueue(name="same-name")],
            **aws_credentials,
        )


async def test_when_single_queue_dict_given__then_it_becomes_the_default_queue(
    aws_credentials: AWSCredentials,
) -> None:
    broker = SQSBroker(queues=SQSQueue(name="my-queue"), **aws_credentials)
    assert broker._default_queue_name == "my-queue"


async def test_when_multiple_queues_given__then_the_first_one_is_the_default(
    aws_credentials: AWSCredentials,
) -> None:
    broker = SQSBroker(
        queues=[SQSQueue(name="first-queue"), SQSQueue(name="second-queue")],
        **aws_credentials,
    )
    assert broker._default_queue_name == "first-queue"
