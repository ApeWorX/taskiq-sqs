---
title: Priority queues
description: >-
  Run a batched low-priority queue and a FIFO urgent queue on the same taskiq-sqs broker,
  worked example included.
---

A single [`SQSBroker`](../index.md) can consume from several queues at once, so you don't need to run separate
workers for different kinds of work. This example combines two features to build a simple priority system:

- a **batched** queue for bulk, low-priority events, where a small delay before delivery is an acceptable
  trade-off for fewer `SendMessage` calls;
- a **FIFO** queue for urgent, time-sensitive alerts that must stay in order per source and get delivered
  immediately.

```python
--8<-- "docs/examples/priority_queues.py"
```

A few things worth noting:

- `process_bulk_event` has no `queue_name` label, so it goes to `bulk-queue` — the first queue in `queues`, and
  therefore the default one.
- `process_urgent_alert` is pinned to `urgent-queue.fifo` via the `queue_name` label on the task decorator itself,
  so every call to `.kiq()` for that task goes there without repeating the label each time.
- `urgent-queue.fifo` enables `ContentBasedDeduplication` through `options` at declare time, since the alerts in
  this example don't set an explicit `deduplication_id` label. Without one or the other, SQS rejects the message.
- Both alerts share `group_id="sensor-1"`, so SQS guarantees they're delivered in the order they were sent —
  "temperature spike" before "temperature back to normal" — something the batched queue makes no promises about.

To run it:

1. Start a worker (it consumes from every configured queue automatically):

    ```bash
    taskiq worker docs.examples.priority_queues:broker
    ```

2. In another terminal, kick the tasks:

    ```bash
    python docs/examples/priority_queues.py
    ```

See [Multiple queues](../index.md#multiple-queues), [FIFO queues](../index.md#fifo-queues) and
[Message batching](../index.md#message-batching) for the reference documentation on each of these features
individually.
