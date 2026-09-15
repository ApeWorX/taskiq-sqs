---
title: Overview
---

This library provides an SQS broker and an S3 result backend for [TaskIQ](https://taskiq-python.github.io/).

## Installation

=== "pip"

    ```bash
    pip install taskiq-sqs
    ```

=== "uv"

    ```bash
    uv add taskiq-sqs
    ```

## Basic usage

Here is an example of how to use the SQS broker with the S3 backend:

```python
import asyncio
from taskiq_sqs import S3ResultBackend, SQSBroker
from taskiq_sqs.types import S3Bucket, SQSQueue

broker = SQSBroker(
    queues=SQSQueue(name="my-queue"),  # by default the broker creates the queue for you if it doesn't exist
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

1. run worker first with `taskiq worker examples.example_broker:broker`
2. after that run broker to create a task and wait for result: `python examples/example_broker.py`

For a set of complete, runnable scenarios that combine several of the features below, see the [Tutorial](tutorial/priority_queues.md).

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

See [Priority queues](tutorial/priority_queues.md) for a worked example combining this with FIFO queues and batching.

## Declaring queues

By default the broker creates a queue on startup if it doesn't exist yet, the same way `S3Bucket` does for buckets. Set `is_declare=False` to require the queue to already exist instead (raises `QueueNotFoundError` if it doesn't). `options` are queue attributes (e.g. `VisibilityTimeout`, `MessageRetentionPeriod`) passed to `CreateQueue`, in AWS's own PascalCase naming, when the queue is declared — they have no effect on a queue that already exists:

```python
from taskiq_sqs import SQSBroker
from taskiq_sqs.types import SQSQueue

broker = SQSBroker(
    queues=SQSQueue(name="my-queue", options={"VisibilityTimeout": "60", "MessageRetentionPeriod": "86400"}),
)
```

FIFO queues get their `FifoQueue` attribute set automatically when declared — no need to include it in `options`.

`S3Bucket` has the same `options` field, for parameters `CreateBucket` accepts beyond `name` (e.g. `acl`), passed through whenever `S3ResultBackend`/`S3OffloadMiddleware` create the bucket.

## Delayed tasks

Set the `delay` label to delay delivery of a task by that many seconds (0-900, SQS's own limit):

```python
from taskiq_sqs import SQSBroker
from taskiq_sqs.types import SQSQueue

broker = SQSBroker(queues=SQSQueue(name="my-queue"))

@broker.task(delay=30)  # "delay" is taskiq_sqs.constants.SQS_DELAY_SECONDS_LABEL
async def send_reminder() -> None:
    ...
```

A value outside the 0-900 range (or not an integer) raises `InvalidDelaySecondsError` when the task is kicked.

!!! note
    Delay can also be combined with [message expiration](#message-expiration) to build a "retry with a deadline"
    pattern — see [Reliable task delivery](tutorial/reliable_task_delivery.md).

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

## Message expiration

Set the `expiry` label to a unix timestamp; if a worker receives the message after that time, it's deleted without being executed:

```python
import time
from taskiq_sqs import SQSBroker
from taskiq_sqs.types import SQSQueue

broker = SQSBroker(queues=SQSQueue(name="my-queue"))

@broker.task()
async def process_event() -> None:
    ...

await process_event.kicker().with_labels(expiry=time.time() + 300).kiq()  # discarded if received after 5 minutes
```

Expiration is checked by the worker on receipt, not by SQS itself — a message can still sit in the queue past its expiry (e.g. while workers are busy or scaled to zero), it just won't run once picked up. `expiry` must be a non-negative number; anything else raises `InvalidExpiryError` when the task is kicked.

## Message batching

Set `is_batching_enabled` on a queue to buffer kicked messages in memory and flush them together via `SendMessageBatch` (up to `batch_size` messages, or after `batch_timeout` seconds, whichever comes first) instead of sending each one immediately:

```python
from taskiq_sqs import SQSBroker
from taskiq_sqs.types import SQSQueue

broker = SQSBroker(
    queues=SQSQueue(name="my-queue", is_batching_enabled=True, batch_size=10, batch_timeout=1.0),
)
```

!!! warning "This trades durability for throughput"
    `kick()` returns as soon as the message is buffered in memory, before it has actually reached SQS. If the
    process crashes within `batch_timeout` seconds, a buffered message is lost even though `.kiq()` already
    returned successfully. `broker.shutdown()` flushes whatever is still pending, so a clean shutdown doesn't
    lose anything — only a crash does.

- Set the `skip_batching` label on a task to send that one message immediately regardless of the queue's setting.
- A `delay` label always sends immediately too — batching doesn't support per-message delay.
- On FIFO queues, messages are grouped by `group_id` before flushing, so a batch never reorders messages within the same group.

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

See [Large payloads with S3](tutorial/large_payloads_with_s3.md) for a complete example that combines offloading with the S3 result backend.
