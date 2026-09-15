import contextlib
import logging
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator
from typing import TYPE_CHECKING

from aiobotocore.session import get_session
from botocore.exceptions import ClientError
from taskiq import AsyncBroker
from taskiq.acks import AckableMessage
from taskiq.message import BrokerMessage

from taskiq_sqs import constants
from taskiq_sqs.exceptions import BrokerInitError
from taskiq_sqs.types import SQSQueue


if TYPE_CHECKING:
    from types_aiobotocore_sqs.client import SQSClient
    from types_aiobotocore_sqs.type_defs import (
        GetQueueUrlResultTypeDef,
        MessageTypeDef,
        SendMessageRequestTypeDef,
    )


logger = logging.getLogger(__name__)


class SQSBroker(AsyncBroker):
    """AWS SQS TaskIQ broker."""

    def __init__(
        self,
        queue_name: str,
        endpoint_url: str | None = None,
        aws_region_name: str = constants.AWS_DEFAULT_REGION,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        wait_time_seconds: int = 0,
        max_number_of_messages: int = 1,
    ) -> None:
        """Initialize the SQS broker.

        :param: queue_name: The name of the SQS queue.
        :param: endpoint_url: The SQS endpoint URL.
        :param aws_region_name: The AWS region name.
        :param aws_access_key_id: The AWS access key ID.
        :param aws_secret_access_key: The AWS secret access key.
        :param: wait_time_seconds: The wait time used for long polling.
        :param: max_number_of_messages: Size of batch to receive from the queue.
        """
        super().__init__()

        self._aws_region = aws_region_name
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._aws_endpoint_url = endpoint_url

        self._session = get_session()
        self._startup_called = False

        self._sqs_queue_url: str | None = None

        if max_number_of_messages > constants.MAX_NUMBER_OF_MESSAGES or max_number_of_messages < 1:
            raise BrokerInitError(details="MaxNumberOfMessages can be no greater than 10 or less than 1")
        self._max_number_of_messages = max_number_of_messages

        if wait_time_seconds > constants.MAX_WAIT_TIME_SECONDS or wait_time_seconds < 0:
            raise BrokerInitError(details="WaitTimeSeconds can be no greater than 20 or less than 0")
        self._wait_time_seconds = wait_time_seconds

        try:
            self._default_queue: SQSQueue = SQSQueue(
                name=queue_name,
                max_number_of_messages=self._max_number_of_messages,
                wait_time_seconds=self._wait_time_seconds,
            )
        except ValueError as error:
            raise BrokerInitError(details="Invalid default queue configuration.") from error

    @contextlib.contextmanager
    def _handle_exceptions(self) -> Generator[None, None, None]:
        """Handle exceptions raised by the SQS client."""
        try:
            yield
        except ClientError as e:
            error = e.response.get("Error", {})
            code = error.get("Code")
            error_message = error.get("Message")
            if code == "AWS.SimpleQueueService.NonExistentQueue":
                raise BrokerInitError(
                    details=f"Queue not found {self._default_queue.name}",
                ) from e
            elif code in ["InvalidParameterValue", "NoSuchBucket"]:
                raise BrokerInitError(details=error_message or "") from e
            else:
                raise BrokerInitError(details=code or "") from e

    async def _get_sqs_client(self) -> "SQSClient":
        self._client_context_creator = self._session.create_client(
            "sqs",
            region_name=self._aws_region,
            endpoint_url=self._aws_endpoint_url,
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
        )
        return await self._client_context_creator.__aenter__()

    async def _close_client(self) -> None:
        """Closes the SQS/S3 client."""
        await self._client_context_creator.__aexit__(None, None, None)

    async def _get_queue_url(self) -> str:
        if not self._sqs_queue_url:
            with self._handle_exceptions():
                queue_result: GetQueueUrlResultTypeDef = await self._sqs_client.get_queue_url(
                    QueueName=self._default_queue.name,
                )
            self._sqs_queue_url = queue_result["QueueUrl"]
        return self._sqs_queue_url

    async def startup(self) -> None:
        """Starts the SQS broker and checks that queue exists."""
        self._startup_called = True
        self._sqs_client = await self._get_sqs_client()

        queue_url = await self._get_queue_url()
        logger.info("Resolved queue '%s' URL: %s", self._default_queue.name, queue_url)

        await super().startup()

    async def shutdown(self) -> None:
        """Shuts down the SQS broker."""
        await self._close_client()
        await super().shutdown()

    async def _build_kick_kwargs(
        self,
        message: BrokerMessage,
    ) -> "SendMessageRequestTypeDef":
        """Build the kwargs for the SQS client kick method.

        This function can be extended by the end user to
        add additional kwargs in the message delivery.
        :param message: BrokerMessage object.
        """
        kwargs: SendMessageRequestTypeDef = {
            "QueueUrl": await self._get_queue_url(),
            "MessageBody": message.message.decode("utf-8"),
        }
        return kwargs

    async def _send_message(
        self,
        message: BrokerMessage,
    ) -> None:
        """Send a single message.

        :param message:
        """
        kwargs = await self._build_kick_kwargs(message)
        with self._handle_exceptions():
            await self._sqs_client.send_message(**kwargs)

    async def kick(self, message: BrokerMessage) -> None:
        """Kick tasks out from current program to configured SQS queue.

        :param message: BrokerMessage object.
        """
        await self._send_message(message)

    def _build_ack_function(
        self,
        queue_url: str,
        receipt_handle: str,
    ) -> Callable[[], Awaitable[None]]:
        """
        This method is used to build an ack for the message.

        :param queue_url: queue url where the message is located
        :param receipt_handle: message to build ack for.
        """

        async def ack() -> None:
            with self._handle_exceptions():
                await self._sqs_client.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=receipt_handle,
                )

        return ack

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """
        This function listens to new messages and yields them.

        :yield: incoming AckableMessages.
        """
        queue_url = await self._get_queue_url()

        while True:
            results = await self._sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=self._max_number_of_messages,
                WaitTimeSeconds=self._wait_time_seconds,
            )
            messages: list[MessageTypeDef] = results.get("Messages", [])

            for message in messages:
                if (body := message.get("Body")) and (receipt_handle := message.get("ReceiptHandle")):
                    yield AckableMessage(
                        data=body.encode("utf-8"),
                        ack=self._build_ack_function(queue_url, receipt_handle),
                    )
