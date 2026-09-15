from typing import NotRequired, TypedDict


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
