import asyncio
import logging
from datetime import datetime, timezone
from typing import (
    AsyncGenerator,
    Callable,
    Optional,
    Union,
)

import boto3
from ape.logging import logger
from asyncer import asyncify
from mypy_boto3_sqs.service_resource import Queue
from taskiq import AsyncBroker
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.acks import AckableMessage
from taskiq.message import BrokerMessage

logger = logging.getLogger(__name__)
stamp = lambda: int(datetime.now(tz=timezone.utc).timestamp())


class SQSBroker(AsyncBroker):
    """AWS SQS TaskIQ broker."""

    def __init__(
        self,
        sqs_queue_url: str,
        result_backend: Optional[AsyncResultBackend] = None,
        task_id_generator: Optional[Callable[[], str]] = None,
    ) -> None:
        super().__init__(result_backend, task_id_generator)
        self.sqs_queue_url = sqs_queue_url
        self._sqs = boto3.resource("sqs")
        self._sqs_queue: Optional[Queue] = None

    async def _get_queue(self) -> Queue:
        queue_name = self.sqs_queue_url.split("/")[-1]

        if not self._sqs_queue:
            self._sqs_queue = await asyncify(self._sqs.get_queue_by_name)(QueueName=queue_name)

            if not self._sqs_queue:
                raise Exception("SQS Queue not found")

        return self._sqs_queue

    async def kick(
        self,
        message: BrokerMessage,
    ) -> None:
        """
        This method is used to kick tasks out from current program.

        Using this method tasks are sent to
        workers.

        You don't need to send broker message. It's helper for brokers,
        please send only bytes from message.message.

        :param message: name of a task.
        """
        queue = await self._get_queue()
        # Must be explicitly set as a label to a unix timestamp
        expiry = message.labels.pop("sqs_expiry", None)

        # SQS structured message attributes
        message_attributes = dict()
        if expiry:
            message_attributes["expiry"] = {
                "StringValue": str(expiry),
                "DataType": "Number",
            }

        await asyncify(queue.send_message)(
            MessageAttributes=message_attributes,
            MessageBody=message.message.decode("utf-8"),
            MessageGroupId=message.task_name,
        )

    async def listen(self) -> AsyncGenerator[Union[bytes, AckableMessage], None]:
        """
        This function listens to new messages and yields them.

        This it the main point for workers.
        This function is used to get new tasks from the network.

        If your broker support acknowledgement, then you
        should wrap your message in AckableMessage dataclass.

        If your messages was wrapped in AckableMessage dataclass,
        taskiq will call ack when finish processing message.

        :yield: incoming messages.
        :return: nothing.
        """

        queue = await self._get_queue()

        # TODO: Consider using AckableMessage and confirm with the queue to reduce lost messages
        while True:
            last_had_message = False

            for message in await asyncify(queue.receive_messages)():
                try:
                    if message.message_attributes:
                        # if expiry was set as a message attribute, respect it
                        if expiry := message.message_attributes.get("expiry"):
                            diff = stamp() - expiry
                            if diff > 0:
                                logger.debug(f"Message expired {diff} seconds ago. Skipping.")
                                continue
                except TypeError:
                    # Ignore weird expiries.  Not critical.
                    pass

                yield message.body.encode("utf-8")
                await asyncify(message.delete)()
                last_had_message = True

            sleepdur = 0.1 if last_had_message else 1
            logger.debug(f"No messages on queue. Broker is sleeping for {sleepdur}s...")
            await asyncio.sleep(sleepdur)
            last_had_message = False
