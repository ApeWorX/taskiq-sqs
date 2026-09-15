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
from taskiq_sqs import S3Bucket, S3ResultBackend, SQSBroker

QUEUE_NAME = "my-queue"
broker = SQSBroker(
    "http://localhost:4566/000000000000/my-queue",  # specify existing queue
    sqs_region_override="us-east-1"
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

## Message expiration

If you set the `sqs_expiry` label to a unix timestamp, the message will be discarded if the worker receives it after that time.

```python
import asyncio
from taskiq_sqs import SQSBroker

broker = SQSBroker("http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/my-queue")

@broker.task
async def add_one(value: int) -> int:
    return value + 1


async def main() -> None:
    # Never forget to call startup in the beginning.
    await broker.startup()
    # Send the task to the broker.
    task = await add_one.kiq(1)
    # Wait for the result. (result backend must be configured)
    result = await task.wait_result(timeout=2)
    print(f"Task execution took: {result.execution_time} seconds.")
    if not result.is_err:
        print(f"Returned value: {result.return_value}")
    else:
        print("Error found while executing task.")
    await broker.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
```

## Offloading large messages to S3

SQS messages are limited to 256 KiB. `S3OffloadMiddleware` transparently uploads task payloads that exceed a configurable threshold to S3 before sending them to the queue, and replaces the message with a reference to the uploaded object. The worker downloads the original payload back from S3 before executing the task, and (by default) removes it from S3 afterwards.

```python
import asyncio
from taskiq_sqs import S3Bucket, S3OffloadMiddleware, SQSBroker

broker = SQSBroker("http://localhost:4566/000000000000/my-queue")
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
