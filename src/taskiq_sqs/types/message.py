from typing import Any

from taskiq_sqs import constants
from taskiq_sqs.exceptions import (
    InvalidDelaySecondsError,
    InvalidExpiryError,
    InvalidMessageDeduplicationIdError,
    InvalidMessageGroupIdError,
)


def validate_delay_seconds(delay_seconds: Any) -> int:
    """Validate a message's delay label."""
    if isinstance(delay_seconds, bool):
        raise InvalidDelaySecondsError(delay_seconds=delay_seconds, max_delay_seconds=constants.MAX_DELAY_SECONDS)
    try:
        numeric = float(delay_seconds)
    except (TypeError, ValueError):
        raise InvalidDelaySecondsError(
            delay_seconds=delay_seconds,
            max_delay_seconds=constants.MAX_DELAY_SECONDS,
        ) from None
    if not numeric.is_integer() or numeric < 0 or numeric > constants.MAX_DELAY_SECONDS:
        raise InvalidDelaySecondsError(delay_seconds=delay_seconds, max_delay_seconds=constants.MAX_DELAY_SECONDS)
    return int(numeric)


def validate_message_group_id(group_id: Any) -> str:
    """Validate a message's group_id label."""
    if not isinstance(group_id, str) or not (1 <= len(group_id) <= constants.MAX_FIFO_ID_LENGTH):
        raise InvalidMessageGroupIdError(group_id=group_id, max_length=constants.MAX_FIFO_ID_LENGTH)
    return group_id


def validate_message_deduplication_id(deduplication_id: Any) -> str:
    """Validate a message's deduplication_id label."""
    if not isinstance(deduplication_id, str) or not (1 <= len(deduplication_id) <= constants.MAX_FIFO_ID_LENGTH):
        raise InvalidMessageDeduplicationIdError(
            deduplication_id=deduplication_id,
            max_length=constants.MAX_FIFO_ID_LENGTH,
        )
    return deduplication_id


def validate_expiry(expiry: Any) -> float:
    """Validate a message's expiry label."""
    if isinstance(expiry, bool):
        raise InvalidExpiryError(expiry=expiry)
    try:
        value = float(expiry)
    except (TypeError, ValueError):
        raise InvalidExpiryError(expiry=expiry) from None
    if value < 0:
        raise InvalidExpiryError(expiry=expiry)
    return value


def is_label_true(value: Any) -> bool:
    """Interpret a boolean-ish message label the way taskiq's own label round-trip does."""
    if isinstance(value, str):
        return value.strip().lower() == "true"
    return bool(value)
