from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True, kw_only=True, frozen=True)
class SQSQueue:
    """Per-queue SQS configuration for SQSBroker.

    Attributes:
        name: The SQS queue name (or "queue-name.fifo" for FIFO queues).
        is_fifo: Whether this is a FIFO queue (default: False).
        max_number_of_messages: Maximum messages to retrieve per poll (1-10,
            default: 1).
        wait_time_seconds: Long polling wait time in seconds (0-20, default: 0).
        visibility_timeout: Optional visibility timeout (in seconds) for received
            messages. While a message is being processed, it remains invisible to
            other consumers.
        options: Optional mapping of additional SQS queue attributes.
    """

    name: str
    max_number_of_messages: int = 1
    wait_time_seconds: int = 0
    options: Mapping[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:  # noqa: D105
        return self.name

    def __hash__(self) -> int:  # noqa: D105
        return hash(self.name)
