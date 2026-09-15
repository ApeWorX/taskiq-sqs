from typing import Final


AWS_DEFAULT_REGION: Final[str] = "us-east-1"

MAX_WAIT_TIME_SECONDS: Final[int] = 20
MAX_NUMBER_OF_MESSAGES: Final[int] = 10
MAX_DELAY_SECONDS: Final[int] = 900
MAX_FIFO_ID_LENGTH: Final[int] = 128

SQS_MAX_MESSAGE_SIZE_BYTES: Final[int] = 262_144
DEFAULT_S3_OFFLOAD_THRESHOLD_BYTES: Final[int] = 200_000

SQS_QUEUE_LABEL: Final[str] = "queue_name"
SQS_DELAY_SECONDS_LABEL: Final[str] = "delay"
SQS_MESSAGE_GROUP_ID_LABEL: Final[str] = "group_id"
SQS_MESSAGE_DEDUPLICATION_ID_LABEL: Final[str] = "deduplication_id"
SQS_EXPIRY_LABEL: Final[str] = "expiry"
