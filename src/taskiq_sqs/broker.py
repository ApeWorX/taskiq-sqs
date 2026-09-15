import asyncio
import contextlib
import logging
import time
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
    QueueNotFoundError,
    UnknownQueueError,
)
from taskiq_sqs.types.message import (
    is_label_true,
    validate_delay_seconds,
    validate_expiry,
    validate_message_deduplication_id,
    validate_message_group_id,
)
from taskiq_sqs.types.queue import SQSQueue, validate_queue


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
        self._batch_queues: dict[str, asyncio.Queue[dict[str, Any]]] = {}
        self._batch_worker_tasks: dict[str, asyncio.Task[None]] = {}

    @staticmethod
    def _normalize_queues(queues: SQSQueue | Sequence[SQSQueue]) -> list[SQSQueue]:
        queue_list = [queues] if isinstance(queues, Mapping) else list(queues)
        if not queue_list:
            raise BrokerInitError(details="At least one queue must be configured.")

        names = [queue["name"] for queue in queue_list]
        if len(names) != len(set(names)):
            raise BrokerInitError(details="Queue names must be unique.")

        for queue in queue_list:
            validate_queue(queue)
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
                queue_url = await self._get_queue_url(queue)
                logger.info("Resolved queue '%s' URL: %s", queue["name"], queue_url)
                if queue.get("is_batching_enabled", False):
                    self._batch_queues[queue["name"]] = asyncio.Queue()
                    self._batch_worker_tasks[queue["name"]] = asyncio.create_task(self._batch_worker(queue))
        except Exception:
            await self._sqs_client.__aexit__(None, None, None)
            raise

        await super().startup()

    async def shutdown(self) -> None:
        """Shuts down the SQS broker."""
        for task in self._batch_worker_tasks.values():
            task.cancel()
        await asyncio.gather(*self._batch_worker_tasks.values(), return_exceptions=True)
        for queue_name, batch_queue in self._batch_queues.items():
            remaining = self._drain_batch_queue(batch_queue)
            if remaining:
                await self._send_batch(self._queues_by_name[queue_name], remaining)
        await self._sqs_client.__aexit__(None, None, None)
        await super().shutdown()

    async def _get_queue_url(self, queue: SQSQueue) -> str:
        name = queue["name"]
        if name not in self._queue_urls:
            result: Any
            try:
                result = await self._sqs_client.get_queue_url(queue_name=name)
            except capo_sqs.errors.QueueDoesNotExist as exc:
                if not queue.get("is_declare", True):
                    raise QueueNotFoundError(queue_name=name) from exc
                result = await self._create_queue(queue)
            except capo_sqs.errors.ServiceError as exc:
                raise BrokerInitError(details=exc.code or "") from exc
            self._queue_urls[name] = result["queue_url"]
        return self._queue_urls[name]

    async def _create_queue(self, queue: SQSQueue) -> Any:
        attributes: dict[Any, Any] = dict(queue.get("options", {}))
        if queue.get("is_fifo", queue["name"].endswith(".fifo")):
            attributes.setdefault("FifoQueue", "true")
        try:
            return await self._sqs_client.create_queue(queue_name=queue["name"], attributes=attributes or None)
        except capo_sqs.errors.ServiceError as exc:
            raise BrokerInitError(details=exc.code or "") from exc

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
            kwargs["delay_seconds"] = validate_delay_seconds(message.labels[constants.SQS_DELAY_SECONDS_LABEL])
        if is_fifo:
            group_id = message.labels.get(constants.SQS_MESSAGE_GROUP_ID_LABEL, message.task_name)
            kwargs["message_group_id"] = validate_message_group_id(group_id)
            if constants.SQS_MESSAGE_DEDUPLICATION_ID_LABEL in message.labels:
                deduplication_id = message.labels[constants.SQS_MESSAGE_DEDUPLICATION_ID_LABEL]
                kwargs["message_deduplication_id"] = validate_message_deduplication_id(deduplication_id)
        if constants.SQS_EXPIRY_LABEL in message.labels:
            expiry = validate_expiry(message.labels[constants.SQS_EXPIRY_LABEL])
            kwargs["message_attributes"] = {
                constants.SQS_EXPIRY_LABEL: {"data_type": "Number", "string_value": str(expiry)},
            }
        return kwargs

    def _should_batch(self, queue: SQSQueue, message: BrokerMessage) -> bool:
        if not queue.get("is_batching_enabled", False):
            return False
        if is_label_true(message.labels.get(constants.SQS_SKIP_BATCHING_LABEL, False)):
            return False
        return constants.SQS_DELAY_SECONDS_LABEL not in message.labels

    async def kick(self, message: BrokerMessage) -> None:
        """Kick tasks out from current program to configured SQS queue."""
        queue = self._resolve_queue(message.labels.get(constants.SQS_QUEUE_LABEL))
        queue_url = await self._get_queue_url(queue)
        kwargs = await self._build_kick_kwargs(message, queue, queue_url)
        if self._should_batch(queue, message):
            await self._batch_queues[queue["name"]].put(kwargs)
            return
        with self._handle_exceptions(queue["name"]):
            await self._sqs_client.send_message(**kwargs)

    @staticmethod
    def _drain_batch_queue(batch_queue: "asyncio.Queue[dict[str, Any]]") -> list[dict[str, Any]]:
        drained = []
        while not batch_queue.empty():
            try:
                drained.append(batch_queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        return drained

    async def _send_batch_to_sqs(self, queue: SQSQueue, queue_url: str, batch: list[dict[str, Any]]) -> None:
        entries: list[Any] = [
            {"id": str(index), **{key: value for key, value in kwargs.items() if key != "queue_url"}}
            for index, kwargs in enumerate(batch)
        ]
        with self._handle_exceptions(queue["name"]):
            response = await self._sqs_client.send_message_batch(queue_url=queue_url, entries=entries)
        for failure in response.get("failed", []):
            logger.error(
                "Failed to send batched message to queue '%s': %s (%s)",
                queue["name"],
                failure.get("message"),
                failure.get("code"),
            )

    async def _send_batch(self, queue: SQSQueue, batch: list[dict[str, Any]]) -> None:
        queue_url = await self._get_queue_url(queue)
        if not queue.get("is_fifo", queue["name"].endswith(".fifo")):
            await self._send_batch_to_sqs(queue, queue_url, batch)
            return
        # Keep each FIFO group's messages together in their own batch call, to preserve their relative order.
        groups: dict[str, list[dict[str, Any]]] = {}
        for kwargs in batch:
            groups.setdefault(kwargs.get("message_group_id", ""), []).append(kwargs)
        for group in groups.values():
            await self._send_batch_to_sqs(queue, queue_url, group)

    async def _batch_worker(self, queue: SQSQueue) -> None:
        """Buffer kicked messages for queue and flush them together via batch send."""
        batch_queue = self._batch_queues[queue["name"]]
        batch_size = queue.get("batch_size", constants.DEFAULT_BATCH_SIZE)
        batch_timeout = queue.get("batch_timeout", constants.DEFAULT_BATCH_TIMEOUT)
        batch: list[dict[str, Any]] = []
        try:
            while True:
                batch = [await batch_queue.get()]
                deadline = time.monotonic() + batch_timeout
                while len(batch) < batch_size:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    try:
                        batch.append(await asyncio.wait_for(batch_queue.get(), timeout=remaining))
                    except TimeoutError:
                        break
                await self._send_batch(queue, batch)
                batch = []
        except asyncio.CancelledError:
            for kwargs in batch:
                batch_queue.put_nowait(kwargs)
            raise

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

    @staticmethod
    def _is_expired(message: Mapping[str, Any]) -> bool:
        expiry_attribute = message.get("message_attributes", {}).get(constants.SQS_EXPIRY_LABEL)
        if expiry_attribute is None:
            return False
        string_value = expiry_attribute.get("string_value")
        if string_value is None:
            return False
        try:
            expiry = float(string_value)
        except ValueError:
            return False
        return time.time() > expiry

    async def _poll_queue(self, queue: SQSQueue, incoming: "asyncio.Queue[_QueueItem]") -> None:
        """Continuously receive messages from a single queue and forward them to the shared incoming queue."""
        try:
            queue_url = await self._get_queue_url(queue)
            while True:
                with self._handle_exceptions(queue["name"]):
                    results = await self._sqs_client.receive_message(
                        queue_url=queue_url,
                        max_number_of_messages=queue.get("max_number_of_messages", 1),
                        wait_time_seconds=queue.get("wait_time_seconds", 0),
                        message_attribute_names=[constants.SQS_EXPIRY_LABEL],
                    )
                for message in results.get("messages", []):
                    body = message.get("body")
                    receipt_handle = message.get("receipt_handle")
                    if not (body and receipt_handle):
                        continue
                    if self._is_expired(message):
                        logger.info("Discarding expired message from queue '%s'", queue["name"])
                        with self._handle_exceptions(queue["name"]):
                            await self._sqs_client.delete_message(queue_url=queue_url, receipt_handle=receipt_handle)
                        continue
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
