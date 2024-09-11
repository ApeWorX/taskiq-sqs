from taskiq_sqs import SQSBroker


def test_init():
    broker = SQSBroker("https://sqs.us-west-2.amazonaws.com/123456789012/queue-name")
    assert (
        broker.sqs_queue_url
        == "https://sqs.us-west-2.amazonaws.com/123456789012/queue-name"
    )
    assert broker.force_ecs_container_credentials is False
    assert broker.sqs_region_override is None
    assert broker._sqs_queue is None
