import pytest

from tests.conftest import AWSCredentials

from taskiq_sqs import SQSBroker
from taskiq_sqs.exceptions import BrokerInitError
from taskiq_sqs.types import SQSQueue


async def test_get_queue_url_client_error(aws_credentials: AWSCredentials) -> None:
    broker = SQSBroker(queues=SQSQueue(name="nonexistent-queue"), **aws_credentials)
    with pytest.raises(BrokerInitError):
        await broker.startup()


async def test_max_number_of_messages_error(aws_credentials: AWSCredentials) -> None:
    with pytest.raises(BrokerInitError):
        SQSBroker(
            queues=SQSQueue(name="nonexistent-queue", max_number_of_messages=15),
            **aws_credentials,
        )


async def test_wait_time_seconds_error(aws_credentials: AWSCredentials) -> None:
    with pytest.raises(BrokerInitError):
        SQSBroker(
            queues=SQSQueue(name="nonexistent-queue", wait_time_seconds=21),
            **aws_credentials,
        )


async def test_when_no_queues_given__then_should_raise_an_error(aws_credentials: AWSCredentials) -> None:
    with pytest.raises(BrokerInitError):
        SQSBroker(queues=[], **aws_credentials)


async def test_when_duplicate_queue_names__then_should_raise_an_error(aws_credentials: AWSCredentials) -> None:
    with pytest.raises(BrokerInitError):
        SQSBroker(
            queues=[SQSQueue(name="same-name"), SQSQueue(name="same-name")],
            **aws_credentials,
        )


async def test_when_single_queue_dict_given__then_it_becomes_the_default_queue(
    aws_credentials: AWSCredentials,
) -> None:
    broker = SQSBroker(queues=SQSQueue(name="my-queue"), **aws_credentials)
    assert broker._default_queue_name == "my-queue"


async def test_when_multiple_queues_given__then_the_first_one_is_the_default(
    aws_credentials: AWSCredentials,
) -> None:
    broker = SQSBroker(
        queues=[SQSQueue(name="first-queue"), SQSQueue(name="second-queue")],
        **aws_credentials,
    )
    assert broker._default_queue_name == "first-queue"
