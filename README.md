# taskiq-sqs

[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/taskiq-sqs?style=for-the-badge&logo=python)](https://pypi.org/project/taskiq-sqs/)
[![PyPI](https://img.shields.io/pypi/v/taskiq-sqs?style=for-the-badge&logo=pypi)](https://pypi.org/project/taskiq-sqs/)
[![Checks](https://img.shields.io/github/check-runs/taskiq-python/taskiq-sqs/main?nameFilter=test%20(ubuntu-latest,%203.12)&style=for-the-badge)](https://github.com/taskiq-python/taskiq-sqs)

This library provides SQS broker and S3 result backend for TaskIQ.

## Installation

```bash
pip install taskiq-sqs
```

## Basic usage

Here is an example of how to use the SQS broker with the S3 backend:

```python
import asyncio
from taskiq_sqs import S3ResultBackend, SQSBroker
from taskiq_sqs.types import S3Bucket, SQSQueue

broker = SQSBroker(
    queues=SQSQueue(name="my-queue"),  # specify an existing queue
    endpoint_url="http://localhost:4566",
    aws_region_name="us-east-1",
).with_result_backend(
    S3ResultBackend(
        bucket=S3Bucket(name="response-bucket")  # by default backend will create bucket for you if it does not exist
    )
)

@broker.task()
async def i_love_aws() -> None:
    await asyncio.sleep(1)
    print("Hello there!")

async def main() -> None:
    await broker.startup()
    task = await i_love_aws.kiq()
    print(await task.wait_result())
    await broker.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
```

How to run:
- run worker first with `taskiq worker examples.example_broker:broker`
- after that run broker to create a task and wait for result: `python examples/example_broker.py`

## Multiple queues

`SQSBroker` accepts a single queue or a list of them. The first queue is the default one, used whenever a task doesn't say otherwise. To send a task to a specific queue, set the `queue_name` label with that queue's name:

```python
from taskiq_sqs import SQSBroker
from taskiq_sqs.types import SQSQueue

broker = SQSBroker(
    queues=[
        SQSQueue(name="default-queue"),
        SQSQueue(name="high-priority-queue", wait_time_seconds=5),
    ],
)

@broker.task(queue_name="high-priority-queue")
async def urgent_task() -> None:
    ...
```

A worker started against this broker consumes from every configured queue at once. Passing a queue name through the `queue_name` label that isn't configured on the broker raises `UnknownQueueError`.

## Delayed tasks

Set the `delay` label to delay delivery of a task by that many seconds (0-900, SQS's own limit)

```python
from taskiq_sqs import SQSBroker
from taskiq_sqs.types import SQSQueue

broker = SQSBroker(queues=SQSQueue(name="my-queue"))

@broker.task(delay=30)  # "delay" is taskiq_sqs.constants.SQS_DELAY_SECONDS_LABEL
async def send_reminder() -> None:
    ...
```

A value outside the 0-900 range (or not an integer) raises `InvalidDelaySecondsError` when the task is kicked.

## FIFO queues

A queue whose name ends in `.fifo` is treated as a FIFO queue automatically, matching SQS's own naming rule (`SQSQueue`'s `is_fifo` field only needs to be set to override that default, and the queue's name must still end in `.fifo` for the broker to accept it as FIFO).

```python
from taskiq_sqs import SQSBroker
from taskiq_sqs.types import SQSQueue

broker = SQSBroker(queues=SQSQueue(name="my-queue.fifo"))

@broker.task(group_id="orders")  # defaults to the task name if not set
async def process_order() -> None:
    ...
```

- `group_id` picks the message's `MessageGroupId` (required by SQS for every FIFO message); it defaults to the task's name.
- `deduplication_id` sets `MessageDeduplicationId`; if not set, the queue must have content-based deduplication enabled, or SQS rejects the message.
- The `delay` label (see [Delayed tasks](#delayed-tasks)) is not supported on FIFO queues — SQS only allows delay to be configured on the queue itself, not per message — and raises `FifoDelayNotSupportedError` if used.

## Offloading large messages to S3

SQS messages are limited to 256 KiB. `S3OffloadMiddleware` transparently uploads task payloads that exceed a configurable threshold to S3 before sending them to the queue, and replaces the message with a reference to the uploaded object. The worker downloads the original payload back from S3 before executing the task, and (by default) removes it from S3 afterwards.

```python
import asyncio
from taskiq_sqs import S3OffloadMiddleware, SQSBroker
from taskiq_sqs.types import S3Bucket, SQSQueue

broker = SQSBroker(queues=SQSQueue(name="my-queue"))
broker.add_middlewares(
    S3OffloadMiddleware(
        bucket=S3Bucket(name="offload-bucket"),  # created automatically if it doesn't exist
        max_message_size=200_000,  # payloads larger than this many bytes are offloaded to S3
    ),
)

@broker.task
async def process_document(content: str) -> int:
    return len(content)


async def main() -> None:
    await broker.startup()
    await process_document.kiq("x" * 1_000_000)  # too large for SQS, transparently offloaded to S3
    await broker.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
```
