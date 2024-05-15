# TaskIQ SQS Broker

Mostly generic SQS async broker for TaskIQ. 

## Expiration

If you set the `sqs_expiry` label to a unix timestamp, the message will be discarded if the worker receives it after that time.
