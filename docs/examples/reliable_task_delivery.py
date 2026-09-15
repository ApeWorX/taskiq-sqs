"""
Run worker:
    taskiq worker docs.examples.reliable_task_delivery:broker

Run this script:
    python docs/examples/reliable_task_delivery.py
"""

import asyncio
import time

import dotenv

from taskiq_sqs import SQSBroker
from taskiq_sqs.types import SQSQueue


dotenv.load_dotenv()

ENDPOINT_URL = "http://localhost:4566"
AWS_REGION = "us-east-1"

broker = SQSBroker(queues=SQSQueue(name="retry-queue"), endpoint_url=ENDPOINT_URL, aws_region_name=AWS_REGION)


@broker.task()
async def send_verification_email(user_id: int) -> None:
    print(f"Sent verification email to user {user_id}")


async def schedule_with_deadline(user_id: int, *, delay_seconds: int, deadline_seconds: float) -> None:
    """Deliver after `delay_seconds`, but give up if no worker picks it up within `deadline_seconds` of now.

    `deadline_seconds` must be greater than `delay_seconds` — the message isn't even visible to a worker
    before `delay_seconds` elapses, so a smaller deadline would make it expire before anyone can receive it.
    """
    await (
        send_verification_email.kicker()
        .with_labels(
            delay=delay_seconds,
            expiry=time.time() + deadline_seconds,
        )
        .kiq(user_id)
    )


async def main() -> None:
    await broker.startup()
    # deliver in 5 seconds, but only if a worker actually picks it up within 30 seconds of being kicked
    await schedule_with_deadline(user_id=42, delay_seconds=5, deadline_seconds=30)
    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
