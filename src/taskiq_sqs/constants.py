from typing import Final


AWS_DEFAULT_REGION: Final[str] = "us-east-1"

MAX_WAIT_TIME_SECONDS: Final[int] = 20
MAX_NUMBER_OF_MESSAGES: Final[int] = 10

SQS_MAX_MESSAGE_SIZE_BYTES: Final[int] = 262_144
DEFAULT_S3_OFFLOAD_THRESHOLD_BYTES: Final[int] = 200_000
