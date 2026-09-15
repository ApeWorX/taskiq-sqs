from taskiq.exceptions import TaskiqError


class BaseTaskiqSQSError(TaskiqError):
    """Base error from taskiq-sqs."""


class BrokerInitError(BaseTaskiqSQSError):
    """Error during broker initialization."""

    __template__ = "Error during broker initialization: {details}"
    details: str


class InvalidEnvironmentError(BaseTaskiqSQSError):
    """Error in case something wrong with environment variables."""

    __template__ = "Something wrong with env: {details}"
    details: str


class ResultBackendError(BaseTaskiqSQSError):
    """Base error for all taskiq-aio-sqs broker exceptions."""

    __template__ = "Unexpected error occurred: {code}"
    code: str | None = None


class BucketNotFoundError(BaseTaskiqSQSError):
    """Error if bucket not found."""

    __template__ = "Bucket '{bucket_name}' not found during initialization and declare=False"
    bucket_name: str


class ResultIsMissingError(BaseTaskiqSQSError):
    """Error if there is no result when we trying to get it."""

    __template__ = "Result for task {task_id} is missing in the result backend"
    task_id: str


class OffloadedPayloadMissingError(BaseTaskiqSQSError):
    """Error if a message references an S3-offloaded payload that can't be found."""

    __template__ = "Offloaded payload for task {task_id} is missing in bucket '{bucket_name}' (key: {key})"
    task_id: str
    bucket_name: str
    key: str


class UnknownQueueError(BaseTaskiqSQSError):
    """Error if a message references a queue that isn't configured on the broker."""

    __template__ = "Message references queue '{queue_name}' which is not configured on this broker"
    queue_name: str


class InvalidDelaySecondsError(BaseTaskiqSQSError):
    """Error if a message's delay label is outside SQS's allowed range."""

    __template__ = "delay must be an integer between 0 and {max_delay_seconds}, got {delay_seconds!r}"
    delay_seconds: object
    max_delay_seconds: int


class FifoDelayNotSupportedError(BaseTaskiqSQSError):
    """Error if a per-message delay is requested on a FIFO queue.

    FIFO queues don't support delaying individual messages; delay can only be configured on the queue itself.
    """

    __template__ = "Per-message delay is not supported on FIFO queue '{queue_name}'; set it on the queue itself"
    queue_name: str


class InvalidMessageGroupIdError(BaseTaskiqSQSError):
    """Error if a message's group_id label is invalid for a FIFO queue."""

    __template__ = "group_id must be a non-empty string of at most {max_length} characters, got {group_id!r}"
    group_id: object
    max_length: int


class InvalidMessageDeduplicationIdError(BaseTaskiqSQSError):
    """Error if a message's deduplication_id label is invalid for a FIFO queue."""

    __template__ = (
        "deduplication_id must be a non-empty string of at most {max_length} characters, got {deduplication_id!r}"
    )
    deduplication_id: object
    max_length: int
