"""
Run worker:
    taskiq worker docs.examples.large_payloads_with_s3:broker

Run this script:
    python docs/examples/large_payloads_with_s3.py
"""

import asyncio

import dotenv

from taskiq_sqs import S3OffloadMiddleware, S3ResultBackend, SQSBroker
from taskiq_sqs.types import S3Bucket, SQSQueue


dotenv.load_dotenv()

ENDPOINT_URL = "http://localhost:4566"
AWS_REGION = "us-east-1"

broker = SQSBroker(
    queues=SQSQueue(name="large-payload-queue"),
    endpoint_url=ENDPOINT_URL,
    aws_region_name=AWS_REGION,
).with_result_backend(
    S3ResultBackend(
        bucket=S3Bucket(name="large-payload-results"),
        endpoint_url=ENDPOINT_URL,
        aws_region_name=AWS_REGION,
    ),
)
broker.add_middlewares(
    S3OffloadMiddleware(
        bucket=S3Bucket(name="large-payload-offload"),
        max_message_size=200_000,  # payloads larger than this many bytes are offloaded to S3
        endpoint_url=ENDPOINT_URL,
        aws_region_name=AWS_REGION,
    ),
)


@broker.task()
async def summarize_document(content: str) -> dict[str, int]:
    return {"characters": len(content), "words": len(content.split())}


async def main() -> None:
    await broker.startup()
    document = "taskiq-sqs " * 100_000  # well over the 256 KiB SQS message limit
    task = await summarize_document.kiq(document)
    result = await task.wait_result(timeout=10)
    print(result.return_value)
    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
