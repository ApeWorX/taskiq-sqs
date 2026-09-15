import capo_sqs

from taskiq_sqs import SQSBroker


async def test_when_listen__than_we_should_delete_message_from_queue(
    sqs_broker: SQSBroker,
    sqs_client: capo_sqs.AsyncSQSClient,
    sqs_queue: str,
) -> None:
    await sqs_client.send_message(queue_url=sqs_queue, message_body="test_message")

    messages = []
    generator = sqs_broker.listen()
    try:
        async for message in generator:
            messages.append(message)
            await message.ack()
            break
    finally:
        await generator.aclose()

    assert len(messages) == 1
    assert messages[0].data == b"test_message"

    response = await sqs_client.receive_message(queue_url=sqs_queue)
    assert not response.get("messages")


async def test_when_listen_with_multiple_queues__then_messages_from_both_are_received(
    multiqueue_sqs_broker: SQSBroker,
    sqs_client: capo_sqs.AsyncSQSClient,
    sqs_queue: str,
    sqs_second_queue: str,
) -> None:
    await sqs_client.send_message(queue_url=sqs_queue, message_body="from_first_queue")
    await sqs_client.send_message(queue_url=sqs_second_queue, message_body="from_second_queue")

    messages = []
    generator = multiqueue_sqs_broker.listen()
    try:
        async for message in generator:
            messages.append(message)
            await message.ack()
            if len(messages) == 2:
                break
    finally:
        await generator.aclose()

    assert {message.data for message in messages} == {b"from_first_queue", b"from_second_queue"}
