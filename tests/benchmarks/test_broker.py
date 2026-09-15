from collections.abc import AsyncGenerator

import pytest
from taskiq import BrokerMessage
from taskiq.acks import AckableMessage

from .conftest import BATCH_SIZE, ack
from taskiq_sqs import SQSBroker


@pytest.mark.benchmark
async def test_build_kick_kwargs(bench_broker: SQSBroker, broker_message: BrokerMessage) -> None:
    queue = bench_broker._resolve_queue(None)
    queue_url = await bench_broker._get_queue_url(queue)
    await bench_broker._build_kick_kwargs(broker_message, queue, queue_url)


@pytest.mark.benchmark
async def test_kick(bench_broker: SQSBroker, broker_message: BrokerMessage) -> None:
    await bench_broker.kick(broker_message)


@pytest.mark.benchmark
async def test_kick_and_listen_roundtrip(
    bench_broker: SQSBroker,
    broker_message: BrokerMessage,
    listener: AsyncGenerator[AckableMessage, None],
) -> None:
    await bench_broker.kick(broker_message)
    message = await anext(listener)
    await ack(message)


@pytest.mark.benchmark
async def test_kick_and_listen_roundtrip_batch(
    bench_broker: SQSBroker,
    broker_message: BrokerMessage,
    listener: AsyncGenerator[AckableMessage, None],
) -> None:
    for _ in range(BATCH_SIZE):
        await bench_broker.kick(broker_message)
    # SQS may return fewer messages than requested per call, so keep pulling until the whole batch is back.
    for _ in range(BATCH_SIZE):
        message = await anext(listener)
        await ack(message)
