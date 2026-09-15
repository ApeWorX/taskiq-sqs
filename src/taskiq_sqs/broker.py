import asyncio
import contextlib
import logging
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator, Mapping, Sequence
from typing import Any

import capo_sqs
from taskiq import AsyncBroker
from taskiq.acks import AckableMessage
from taskiq.message import BrokerMessage

from taskiq_sqs import constants
from taskiq_sqs.exceptions import (
    BrokerInitError,
    FifoDelayNotSupportedError,
    InvalidDelaySecondsError,
    InvalidMessageDeduplicationIdError,
    InvalidMessageGroupIdError,
    UnknownQueueError,
)
from taskiq_sqs.types import SQSQueue


logger = logging.getLogger(__name__)

_QueueItem = AckableMessage | BaseException


class SQSBroker(AsyncBroker):
    """AWS SQS TaskIQ broker."""

    def __init__(
        self,
        queues: SQSQueue | Sequence[SQSQueue],
        endpoint_url: str | None = None,
        aws_region_name: str = constants.AWS_DEFAULT_REGION,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
    ) -> None:
        """Initialize the SQS broker.

        :param queues: a single queue configuration, or a sequence of them for multiqueue support.
        :param endpoint_url: the SQS endpoint URL.
        :param aws_region_name: the AWS region name.
        :param aws_access_key_id: the AWS access key ID.
        :param aws_secret_access_key: the AWS secret access key.
        """
        super().__init__()

        self._aws_region = aws_region_name
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._aws_endpoint_url = endpoint_url

        self._queues = self._normalize_queues(queues)
        self._default_queue_name = self._queues[0]["name"]
        self._queues_by_name = {queue["name"]: queue for queue in self._queues}
        self._queue_urls: dict[str, str] = {}

    @staticmethod
    def _normalize_queues(queues: SQSQueue | Sequence[SQSQueue]) -> list[SQSQueue]:
        queue_list = [queues] if isinstance(queues, Mapping) else list(queues)
        if not queue_list:
            raise BrokerInitError(details="At least one queue must be configured.")

        names = [queue["name"] for queue in queue_list]
        if len(names) != len(set(names)):
            raise BrokerInitError(details="Queue names must be unique.")

        for queue in queue_list:
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
        return queue_list

    def _resolve_queue(self, queue_name: str | None) -> SQSQueue:
        name = queue_name or self._default_queue_name
        queue = self._queues_by_name.get(name)
        if queue is None:
            raise UnknownQueueError(queue_name=name)
        return queue

    @contextlib.contextmanager
    def _handle_exceptions(self, queue_name: str) -> Generator[None, None, None]:
        """Handle exceptions raised by the SQS client."""
        try:
            yield
        except capo_sqs.errors.QueueDoesNotExist as e:
            raise BrokerInitError(details=f"Queue not found {queue_name}") from e
        except capo_sqs.errors.ServiceError as e:
            raise BrokerInitError(details=e.code or "") from e

    async def startup(self) -> None:
        """Starts the SQS broker and checks that every configured queue exists."""
        credentials = None
        if self._aws_access_key_id and self._aws_secret_access_key:
            credentials = capo_sqs.Credentials(
                access_key=self._aws_access_key_id,
                secret_key=self._aws_secret_access_key,
            )
        self._sqs_client = capo_sqs.AsyncSQSClient(
            region=self._aws_region,
            endpoint=self._aws_endpoint_url,
            credentials=credentials,
        )
        await self._sqs_client.__aenter__()
        try:
            for queue in self._queues:
                queue_url = await self._get_queue_url(queue["name"])
                logger.info("Resolved queue '%s' URL: %s", queue["name"], queue_url)
        except Exception:
            await self._sqs_client.__aexit__(None, None, None)
            raise

        await super().startup()

    async def shutdown(self) -> None:
        """Shuts down the SQS broker."""
        await self._sqs_client.__aexit__(None, None, None)
        await super().shutdown()

    async def _get_queue_url(self, queue_name: str) -> str:
        if queue_name not in self._queue_urls:
            with self._handle_exceptions(queue_name):
                result = await self._sqs_client.get_queue_url(queue_name=queue_name)
            self._queue_urls[queue_name] = result["queue_url"]
        return self._queue_urls[queue_name]

    async def _build_kick_kwargs(
        self,
        message: BrokerMessage,
        queue: SQSQueue,
        queue_url: str,
    ) -> dict[str, Any]:
        """Build the kwargs for the SQS client kick method.

        This function can be extended by the end user to add additional kwargs in the message delivery.
        :param message: BrokerMessage object.
        :param queue: the queue the message will be sent to.
        :param queue_url: URL of the queue the message will be sent to.
        """
        kwargs: dict[str, Any] = {
            "queue_url": queue_url,
            "message_body": message.message.decode("utf-8"),
        }
        is_fifo = queue.get("is_fifo", queue["name"].endswith(".fifo"))
        if constants.SQS_DELAY_SECONDS_LABEL in message.labels:
            if is_fifo:
                raise FifoDelayNotSupportedError(queue_name=queue["name"])
            kwargs["delay_seconds"] = self._validate_delay_seconds(message.labels[constants.SQS_DELAY_SECONDS_LABEL])
        if is_fifo:
            group_id = message.labels.get(constants.SQS_MESSAGE_GROUP_ID_LABEL, message.task_name)
            kwargs["message_group_id"] = self._validate_message_group_id(group_id)
            if constants.SQS_MESSAGE_DEDUPLICATION_ID_LABEL in message.labels:
                deduplication_id = message.labels[constants.SQS_MESSAGE_DEDUPLICATION_ID_LABEL]
                kwargs["message_deduplication_id"] = self._validate_message_deduplication_id(deduplication_id)
        return kwargs

    @staticmethod
    def _validate_delay_seconds(delay_seconds: Any) -> int:
        if isinstance(delay_seconds, bool) or not isinstance(delay_seconds, int):
            raise InvalidDelaySecondsError(delay_seconds=delay_seconds, max_delay_seconds=constants.MAX_DELAY_SECONDS)
        if delay_seconds < 0 or delay_seconds > constants.MAX_DELAY_SECONDS:
            raise InvalidDelaySecondsError(delay_seconds=delay_seconds, max_delay_seconds=constants.MAX_DELAY_SECONDS)
        return delay_seconds

    @staticmethod
    def _validate_message_group_id(group_id: Any) -> str:
        if not isinstance(group_id, str) or not (1 <= len(group_id) <= constants.MAX_FIFO_ID_LENGTH):
            raise InvalidMessageGroupIdError(group_id=group_id, max_length=constants.MAX_FIFO_ID_LENGTH)
        return group_id

    @staticmethod
    def _validate_message_deduplication_id(deduplication_id: Any) -> str:
        if not isinstance(deduplication_id, str) or not (1 <= len(deduplication_id) <= constants.MAX_FIFO_ID_LENGTH):
            raise InvalidMessageDeduplicationIdError(
                deduplication_id=deduplication_id,
                max_length=constants.MAX_FIFO_ID_LENGTH,
            )
        return deduplication_id

    async def kick(self, message: BrokerMessage) -> None:
        """Kick tasks out from current program to configured SQS queue."""
        queue = self._resolve_queue(message.labels.get(constants.SQS_QUEUE_LABEL))
        queue_url = await self._get_queue_url(queue["name"])
        kwargs = await self._build_kick_kwargs(message, queue, queue_url)
        with self._handle_exceptions(queue["name"]):
            await self._sqs_client.send_message(**kwargs)

    def _build_ack_function(
        self,
        queue_name: str,
        queue_url: str,
        receipt_handle: str,
    ) -> Callable[[], Awaitable[None]]:
        """
        This method is used to build an ack for the message.

        :param queue_name: name of the queue where the message is located.
        :param queue_url: queue url where the message is located.
        :param receipt_handle: message to build ack for.
        """

        async def ack() -> None:
            with self._handle_exceptions(queue_name):
                await self._sqs_client.delete_message(
                    queue_url=queue_url,
                    receipt_handle=receipt_handle,
                )

        return ack

    async def _poll_queue(self, queue: SQSQueue, incoming: "asyncio.Queue[_QueueItem]") -> None:
        """Continuously receive messages from a single queue and forward them to the shared incoming queue."""
        try:
            queue_url = await self._get_queue_url(queue["name"])
            while True:
                with self._handle_exceptions(queue["name"]):
                    results = await self._sqs_client.receive_message(
                        queue_url=queue_url,
                        max_number_of_messages=queue.get("max_number_of_messages", 1),
                        wait_time_seconds=queue.get("wait_time_seconds", 0),
                    )
                for message in results.get("messages", []):
                    body = message.get("body")
                    receipt_handle = message.get("receipt_handle")
                    if body and receipt_handle:
                        await incoming.put(
                            AckableMessage(
                                data=body.encode("utf-8"),
                                ack=self._build_ack_function(queue["name"], queue_url, receipt_handle),
                            ),
                        )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            await incoming.put(exc)

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """
        This function listens to new messages on every configured queue and yields them.

        :yield: incoming AckableMessages.
        """
        incoming: asyncio.Queue[_QueueItem] = asyncio.Queue()
        pollers = [asyncio.create_task(self._poll_queue(queue, incoming)) for queue in self._queues]
        try:
            while True:
                item = await incoming.get()
                if isinstance(item, BaseException):
                    raise item
                yield item
        finally:
            for task in pollers:
                task.cancel()
            await asyncio.gather(*pollers, return_exceptions=True)
