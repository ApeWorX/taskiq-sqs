import asyncio

import capo_sqs
import pytest
from taskiq import BrokerMessage

from tests.conftest import _queue_name_from_url

from taskiq_sqs import SQSBroker
from taskiq_sqs.constants import SQS_DELAY_SECONDS_LABEL, SQS_QUEUE_LABEL
from taskiq_sqs.exceptions import BrokerInitError, InvalidDelaySecondsError, UnknownQueueError


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


@pytest.mark.parametrize("delay_seconds", [-1, 901, "10", 10.5, True])
async def test_when_kick_called_with_invalid_delay_label__then_should_raise_an_error(
    sqs_broker: SQSBroker,
    broker_message: BrokerMessage,
    delay_seconds: object,
) -> None:
    broker_message.labels[SQS_DELAY_SECONDS_LABEL] = delay_seconds

    with pytest.raises(InvalidDelaySecondsError):
        await sqs_broker.kick(broker_message)
