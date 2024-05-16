# TaskIQ SQS Broker

Mostly generic SQS async broker for TaskIQ. 

## Expiration

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