"""
Run worker:
    taskiq worker docs.examples.priority_queues:broker

Run this script to kick a batch of bulk events and two ordered urgent alerts:
    python docs/examples/priority_queues.py
"""

import asyncio

import dotenv

from taskiq_sqs import SQSBroker
from taskiq_sqs.types import SQSQueue


dotenv.load_dotenv()

ENDPOINT_URL = "http://localhost:4566"
AWS_REGION = "us-east-1"

broker = SQSBroker(
    queues=[
        # bulk, low-priority work: batched together to cut down on SendMessage calls
        SQSQueue(name="bulk-queue", is_batching_enabled=True, batch_size=10, batch_timeout=1.0),
        # time-sensitive work: FIFO so alerts from the same source stay in order. ContentBasedDeduplication is
        # required here since these messages don't set an explicit deduplication_id label.
        SQSQueue(name="urgent-queue.fifo", options={"ContentBasedDeduplication": "true"}),
    ],
    endpoint_url=ENDPOINT_URL,
    aws_region_name=AWS_REGION,
)


@broker.task()
async def process_bulk_event(event_id: int) -> None:
    """Runs on the default queue (the first one in `queues`), since no `queue_name` label is set."""
    print(f"Processed bulk event {event_id}")


@broker.task(queue_name="urgent-queue.fifo")
async def process_urgent_alert(source: str, message: str) -> None:
    print(f"[{source}] {message}")


async def main() -> None:
    await broker.startup()

    for event_id in range(5):
        await process_bulk_event.kiq(event_id)

    # group_id keeps alerts from the same source ordered relative to each other
    await process_urgent_alert.kicker().with_labels(group_id="sensor-1").kiq("sensor-1", "temperature spike")
    await process_urgent_alert.kicker().with_labels(group_id="sensor-1").kiq("sensor-1", "temperature back to normal")

    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
