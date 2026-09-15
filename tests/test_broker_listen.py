import asyncio
import time

import capo_sqs
import pytest
from taskiq import BrokerMessage

from taskiq_sqs import SQSBroker
from taskiq_sqs.constants import SQS_EXPIRY_LABEL


async def test_when_listen__than_we_should_delete_message_from_queue(
    sqs_broker: SQSBroker,
    sqs_client: capo_sqs.AsyncSQSClient,
    sqs_queue: str,
) -> None:
    await sqs_client.send_message(queue_url=sqs_queue, message_body="test_message")

    messages = []
    generator = sqs_broker.listen()
    try:
        async for message in generator:
            messages.append(message)
            await message.ack()
            break
    finally:
        await generator.aclose()

    assert len(messages) == 1
    assert messages[0].data == b"test_message"

    response = await sqs_client.receive_message(queue_url=sqs_queue)
    assert not response.get("messages")


async def test_when_listen_with_multiple_queues__then_messages_from_both_are_received(
    multiqueue_sqs_broker: SQSBroker,
    sqs_client: capo_sqs.AsyncSQSClient,
    sqs_queue: str,
    sqs_second_queue: str,
) -> None:
    await sqs_client.send_message(queue_url=sqs_queue, message_body="from_first_queue")
    await sqs_client.send_message(queue_url=sqs_second_queue, message_body="from_second_queue")

    messages = []
    generator = multiqueue_sqs_broker.listen()
    try:
        async for message in generator:
            messages.append(message)
            await message.ack()
            if len(messages) == 2:
                break
    finally:
        await generator.aclose()

    assert {message.data for message in messages} == {b"from_first_queue", b"from_second_queue"}


async def test_when_message_expired__then_it_is_discarded_without_being_yielded(
    sqs_broker: SQSBroker,
    sqs_client: capo_sqs.AsyncSQSClient,
    sqs_queue: str,
) -> None:
    expired_message = BrokerMessage(
        task_id="expired_task",
        task_name="expired_task",
        message=b"expired_message",
        labels={SQS_EXPIRY_LABEL: time.time() - 10},
    )
    await sqs_broker.kick(expired_message)

    generator = sqs_broker.listen()
    try:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(generator.__anext__(), timeout=2)
    finally:
        await generator.aclose()

    response = await sqs_client.receive_message(queue_url=sqs_queue)
    assert not response.get("messages")


async def test_when_message_not_yet_expired__then_it_is_yielded_normally(
    sqs_broker: SQSBroker,
) -> None:
    live_message = BrokerMessage(
        task_id="live_task",
        task_name="live_task",
        message=b"live_message",
        labels={SQS_EXPIRY_LABEL: time.time() + 60},
    )
    await sqs_broker.kick(live_message)

    generator = sqs_broker.listen()
    try:
        received = await asyncio.wait_for(generator.__anext__(), timeout=2)
        await received.ack()
    finally:
        await generator.aclose()

    assert received.data == b"live_message"
