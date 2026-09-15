from typing import NotRequired, TypedDict

from taskiq_sqs import constants
from taskiq_sqs.exceptions import BrokerInitError


class SQSQueue(TypedDict):
    """
    Represents an SQS queue configuration.

    Attributes:
        name: The SQS queue name.
        max_number_of_messages: Maximum messages to retrieve per poll (1-10). Defaults to 1.
        wait_time_seconds: Long polling wait time in seconds (0-20). Defaults to 0.
        is_fifo: Whether this is a FIFO queue.
        is_batching_enabled: Whether to buffer kicked messages in memory and flush them via batch send.
        batch_size: Maximum messages per batch (1-10). Defaults to 10.
        batch_timeout: Maximum seconds to wait for a batch to fill up before flushing it anyway. Defaults to 1.0.
    """

    name: str
    max_number_of_messages: NotRequired[int]
    wait_time_seconds: NotRequired[int]
    is_fifo: NotRequired[bool]
    is_batching_enabled: NotRequired[bool]
    batch_size: NotRequired[int]
    batch_timeout: NotRequired[float]


def validate_queue(queue: SQSQueue) -> None:
    """Validate a single queue's own fields against SQS's constraints."""
    max_number_of_messages = queue.get("max_number_of_messages", 1)
    if max_number_of_messages > constants.MAX_NUMBER_OF_MESSAGES or max_number_of_messages < 1:
        raise BrokerInitError(
            details=f"MaxNumberOfMessages for queue '{queue['name']}' can be no greater than 10 or less than 1",
        )
    wait_time_seconds = queue.get("wait_time_seconds", 0)
    if wait_time_seconds > constants.MAX_WAIT_TIME_SECONDS or wait_time_seconds < 0:
        raise BrokerInitError(
            details=f"WaitTimeSeconds for queue '{queue['name']}' can be no greater than 20 or less than 0",
        )
    ends_with_fifo_suffix = queue["name"].endswith(".fifo")
    if "is_fifo" in queue and queue["is_fifo"] != ends_with_fifo_suffix:
        raise BrokerInitError(
            details=f"Queue '{queue['name']}' has is_fifo={queue['is_fifo']}, but SQS requires FIFO queue "
            "names to end in '.fifo' and standard queue names not to",
        )
    batch_size = queue.get("batch_size", constants.DEFAULT_BATCH_SIZE)
    if batch_size > constants.MAX_BATCH_SIZE or batch_size < 1:
        raise BrokerInitError(
            details=f"BatchSize for queue '{queue['name']}' can be no greater than 10 or less than 1",
        )
    batch_timeout = queue.get("batch_timeout", constants.DEFAULT_BATCH_TIMEOUT)
    if batch_timeout <= 0:
        raise BrokerInitError(details=f"BatchTimeout for queue '{queue['name']}' must be greater than 0")
