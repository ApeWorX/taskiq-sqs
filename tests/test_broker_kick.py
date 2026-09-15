import asyncio

import capo_sqs
import pytest
from taskiq import BrokerMessage

from tests.conftest import _queue_name_from_url

from taskiq_sqs import SQSBroker
from taskiq_sqs.constants import (
    SQS_DELAY_SECONDS_LABEL,
    SQS_EXPIRY_LABEL,
    SQS_MESSAGE_DEDUPLICATION_ID_LABEL,
    SQS_MESSAGE_GROUP_ID_LABEL,
    SQS_QUEUE_LABEL,
)
from taskiq_sqs.exceptions import (
    BrokerInitError,
    FifoDelayNotSupportedError,
    InvalidDelaySecondsError,
    InvalidExpiryError,
    InvalidMessageDeduplicationIdError,
    InvalidMessageGroupIdError,
    UnknownQueueError,
)


async def test_when_kick_called__than_message_should_be_published_to_queue(
    sqs_broker: SQSBroker,
    sqs_client: capo_sqs.AsyncSQSClient,
    sqs_queue: str,
    broker_message: BrokerMessage,
) -> None:
    await sqs_broker.kick(broker_message)

    response = await sqs_client.receive_message(queue_url=sqs_queue)
    messages = response.get("messages", [])
    assert len(messages) == 1
    assert messages[0].get("body") == "test_message"


async def test_when_during_kick_queue_not_found__then_should_raise_an_error(
    sqs_broker: SQSBroker,
    broker_message: BrokerMessage,
) -> None:
    sqs_broker._queue_urls[sqs_broker._default_queue_name] = "nonexistent-queue"
    with pytest.raises(BrokerInitError):
        await sqs_broker.kick(broker_message)


async def test_when_kick_called_without_queue_label__then_message_goes_to_default_queue(
    multiqueue_sqs_broker: SQSBroker,
    sqs_client: capo_sqs.AsyncSQSClient,
    sqs_queue: str,
    broker_message: BrokerMessage,
) -> None:
    await multiqueue_sqs_broker.kick(broker_message)

    response = await sqs_client.receive_message(queue_url=sqs_queue)
    assert len(response.get("messages", [])) == 1


async def test_when_kick_called_with_queue_label__then_message_goes_to_that_queue(
    multiqueue_sqs_broker: SQSBroker,
    sqs_client: capo_sqs.AsyncSQSClient,
    sqs_second_queue: str,
    broker_message: BrokerMessage,
) -> None:
    broker_message.labels[SQS_QUEUE_LABEL] = _queue_name_from_url(sqs_second_queue)

    await multiqueue_sqs_broker.kick(broker_message)

    response = await sqs_client.receive_message(queue_url=sqs_second_queue)
    assert len(response.get("messages", [])) == 1


async def test_when_kick_called_with_unknown_queue_label__then_should_raise_an_error(
    multiqueue_sqs_broker: SQSBroker,
    broker_message: BrokerMessage,
) -> None:
    broker_message.labels[SQS_QUEUE_LABEL] = "unknown-queue"

    with pytest.raises(UnknownQueueError):
        await multiqueue_sqs_broker.kick(broker_message)


async def test_when_kick_called_with_delay_label__then_message_is_delayed(
    sqs_broker: SQSBroker,
    sqs_client: capo_sqs.AsyncSQSClient,
    sqs_queue: str,
    broker_message: BrokerMessage,
) -> None:
    broker_message.labels[SQS_DELAY_SECONDS_LABEL] = 1

    await sqs_broker.kick(broker_message)

    immediate = await sqs_client.receive_message(queue_url=sqs_queue)
    assert not immediate.get("messages")

    await asyncio.sleep(1.2)

    delayed = await sqs_client.receive_message(queue_url=sqs_queue)
    assert len(delayed.get("messages", [])) == 1


@pytest.mark.parametrize("delay_seconds", [-1, 901, "not-a-number", 10.5, "10.5", True])
async def test_when_kick_called_with_invalid_delay_label__then_should_raise_an_error(
    sqs_broker: SQSBroker,
    broker_message: BrokerMessage,
    delay_seconds: object,
) -> None:
    broker_message.labels[SQS_DELAY_SECONDS_LABEL] = delay_seconds

    with pytest.raises(InvalidDelaySecondsError):
        await sqs_broker.kick(broker_message)


async def test_when_kick_called_with_stringified_delay_label__then_it_is_accepted(
    sqs_broker: SQSBroker,
    sqs_client: capo_sqs.AsyncSQSClient,
    sqs_queue: str,
    broker_message: BrokerMessage,
) -> None:
    broker_message.labels[SQS_DELAY_SECONDS_LABEL] = "1"

    await sqs_broker.kick(broker_message)

    immediate = await sqs_client.receive_message(queue_url=sqs_queue)
    assert not immediate.get("messages")

    await asyncio.sleep(1.2)

    delayed = await sqs_client.receive_message(queue_url=sqs_queue)
    assert len(delayed.get("messages", [])) == 1


async def test_when_task_kicked_through_kicker_with_delay_label__then_it_is_delayed(
    sqs_broker: SQSBroker,
    sqs_client: capo_sqs.AsyncSQSClient,
    sqs_queue: str,
) -> None:
    """End-to-end regression test for the real `@broker.task()` / `.kiq()` path, not a hand-built BrokerMessage."""

    @sqs_broker.task()
    async def sample_task() -> None: ...

    await sample_task.kicker().with_labels(**{SQS_DELAY_SECONDS_LABEL: 1}).kiq()

    immediate = await sqs_client.receive_message(queue_url=sqs_queue)
    assert not immediate.get("messages")

    await asyncio.sleep(1.2)

    delayed = await sqs_client.receive_message(queue_url=sqs_queue)
    assert len(delayed.get("messages", [])) == 1


async def test_when_kick_called_on_standard_queue__then_no_fifo_attributes_are_sent(
    sqs_broker: SQSBroker,
    sqs_client: capo_sqs.AsyncSQSClient,
    sqs_queue: str,
    broker_message: BrokerMessage,
) -> None:
    await sqs_broker.kick(broker_message)

    response = await sqs_client.receive_message(
        queue_url=sqs_queue,
        message_system_attribute_names=["MessageGroupId"],
    )
    messages = response.get("messages", [])
    assert len(messages) == 1
    assert "MessageGroupId" not in messages[0].get("attributes", {})


async def test_when_kick_called_on_fifo_queue_without_group_id_label__then_task_name_is_used(
    fifo_sqs_broker: SQSBroker,
    sqs_client: capo_sqs.AsyncSQSClient,
    fifo_sqs_queue: str,
    broker_message: BrokerMessage,
) -> None:
    await fifo_sqs_broker.kick(broker_message)

    response = await sqs_client.receive_message(
        queue_url=fifo_sqs_queue,
        message_system_attribute_names=["MessageGroupId"],
    )
    messages = response.get("messages", [])
    assert len(messages) == 1
    assert messages[0].get("attributes", {}).get("MessageGroupId") == broker_message.task_name


async def test_when_kick_called_on_fifo_queue_with_group_id_label__then_it_is_used(
    fifo_sqs_broker: SQSBroker,
    sqs_client: capo_sqs.AsyncSQSClient,
    fifo_sqs_queue: str,
    broker_message: BrokerMessage,
) -> None:
    broker_message.labels[SQS_MESSAGE_GROUP_ID_LABEL] = "custom-group"

    await fifo_sqs_broker.kick(broker_message)

    response = await sqs_client.receive_message(
        queue_url=fifo_sqs_queue,
        message_system_attribute_names=["MessageGroupId"],
    )
    messages = response.get("messages", [])
    assert len(messages) == 1
    assert messages[0].get("attributes", {}).get("MessageGroupId") == "custom-group"


async def test_when_kick_called_on_fifo_queue_with_deduplication_id_label__then_it_is_used(
    fifo_sqs_broker: SQSBroker,
    sqs_client: capo_sqs.AsyncSQSClient,
    fifo_sqs_queue: str,
    broker_message: BrokerMessage,
) -> None:
    broker_message.labels[SQS_MESSAGE_DEDUPLICATION_ID_LABEL] = "custom-dedup-id"

    await fifo_sqs_broker.kick(broker_message)

    response = await sqs_client.receive_message(
        queue_url=fifo_sqs_queue,
        message_system_attribute_names=["MessageDeduplicationId"],
    )
    messages = response.get("messages", [])
    assert len(messages) == 1
    assert messages[0].get("attributes", {}).get("MessageDeduplicationId") == "custom-dedup-id"


async def test_when_kick_called_with_delay_on_fifo_queue__then_should_raise_an_error(
    fifo_sqs_broker: SQSBroker,
    broker_message: BrokerMessage,
) -> None:
    broker_message.labels[SQS_DELAY_SECONDS_LABEL] = 5

    with pytest.raises(FifoDelayNotSupportedError):
        await fifo_sqs_broker.kick(broker_message)


@pytest.mark.parametrize("group_id", ["", "x" * 129, 123, None])
async def test_when_kick_called_on_fifo_queue_with_invalid_group_id__then_should_raise_an_error(
    fifo_sqs_broker: SQSBroker,
    broker_message: BrokerMessage,
    group_id: object,
) -> None:
    broker_message.labels[SQS_MESSAGE_GROUP_ID_LABEL] = group_id

    with pytest.raises(InvalidMessageGroupIdError):
        await fifo_sqs_broker.kick(broker_message)


@pytest.mark.parametrize("deduplication_id", ["", "x" * 129, 123, None])
async def test_when_kick_called_on_fifo_queue_with_invalid_deduplication_id__then_should_raise_an_error(
    fifo_sqs_broker: SQSBroker,
    broker_message: BrokerMessage,
    deduplication_id: object,
) -> None:
    broker_message.labels[SQS_MESSAGE_DEDUPLICATION_ID_LABEL] = deduplication_id

    with pytest.raises(InvalidMessageDeduplicationIdError):
        await fifo_sqs_broker.kick(broker_message)


async def test_when_multiple_messages_kicked_to_same_group__then_order_is_preserved(
    fifo_sqs_broker: SQSBroker,
    sqs_client: capo_sqs.AsyncSQSClient,
    fifo_sqs_queue: str,
) -> None:
    for i in range(3):
        message = BrokerMessage(
            task_id=f"task-{i}",
            task_name="ordered_task",
            message=f"message-{i}".encode(),
            labels={
                SQS_MESSAGE_GROUP_ID_LABEL: "same-group",
                SQS_MESSAGE_DEDUPLICATION_ID_LABEL: f"dedup-{i}",
            },
        )
        await fifo_sqs_broker.kick(message)

    response = await sqs_client.receive_message(queue_url=fifo_sqs_queue, max_number_of_messages=3)
    bodies = [message.get("body") for message in response.get("messages", [])]
    assert bodies == ["message-0", "message-1", "message-2"]


@pytest.mark.parametrize("expiry", [-1, "soon", True])
async def test_when_kick_called_with_invalid_expiry_label__then_should_raise_an_error(
    sqs_broker: SQSBroker,
    broker_message: BrokerMessage,
    expiry: object,
) -> None:
    broker_message.labels[SQS_EXPIRY_LABEL] = expiry

    with pytest.raises(InvalidExpiryError):
        await sqs_broker.kick(broker_message)


async def test_when_kick_called_with_stringified_expiry_label__then_it_is_accepted(
    sqs_broker: SQSBroker,
    sqs_client: capo_sqs.AsyncSQSClient,
    sqs_queue: str,
    broker_message: BrokerMessage,
) -> None:
    # same reasoning as test_when_kick_called_with_stringified_delay_label__then_it_is_accepted
    broker_message.labels[SQS_EXPIRY_LABEL] = "1789505020.5"

    await sqs_broker.kick(broker_message)

    response = await sqs_client.receive_message(queue_url=sqs_queue, message_attribute_names=[SQS_EXPIRY_LABEL])
    messages = response.get("messages", [])
    assert len(messages) == 1
    attribute = messages[0].get("message_attributes", {}).get(SQS_EXPIRY_LABEL, {})
    assert attribute.get("string_value") == "1789505020.5"
