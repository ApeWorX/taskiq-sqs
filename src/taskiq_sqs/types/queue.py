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
    """

    name: str
    max_number_of_messages: NotRequired[int]
    wait_time_seconds: NotRequired[int]
    is_fifo: NotRequired[bool]


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
