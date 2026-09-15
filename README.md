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

`SQSBroker` accepts a single queue or a list of them. The first queue is the default one, used whenever a task doesn't say otherwise. To send a task to a specific queue, set the `sqs_queue` label with that queue's name:

```python
from taskiq_sqs import SQSBroker
from taskiq_sqs.types import SQSQueue

broker = SQSBroker(
    queues=[
        SQSQueue(name="default-queue"),
        SQSQueue(name="high-priority-queue", wait_time_seconds=5),
    ],
)

@broker.task(sqs_queue="high-priority-queue")  # "sqs_queue" is taskiq_sqs.constants.SQS_QUEUE_LABEL
async def urgent_task() -> None:
    ...
```

A worker started against this broker consumes from every configured queue at once. Passing a queue name through the `sqs_queue` label that isn't configured on the broker raises `UnknownQueueError`.

## Delayed tasks

Set the `sqs_delay_seconds` label to delay delivery of a task by that many seconds (0-900, SQS's own limit):

```python
from taskiq_sqs import SQSBroker
from taskiq_sqs.types import SQSQueue

broker = SQSBroker(queues=SQSQueue(name="my-queue"))

@broker.task(sqs_delay_seconds=30)  # "sqs_delay_seconds" is taskiq_sqs.constants.SQS_DELAY_SECONDS_LABEL
async def send_reminder() -> None:
    ...
```

A value outside the 0-900 range (or not an integer) raises `InvalidDelaySecondsError` when the task is kicked.

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
