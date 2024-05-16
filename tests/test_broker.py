from taskiq_sqs import SQSBroker


def test_init():
    broker = SQSBroker("https://sqs.us-west-2.amazonaws.com/123456789012/queue-name")
    assert (
        broker.sqs_queue_url
        == "https://sqs.us-west-2.amazonaws.com/123456789012/queue-name"
    )
    assert broker._sqs is not None
    assert broker._sqs_queue is None
