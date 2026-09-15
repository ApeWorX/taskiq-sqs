---
title: Reliable task delivery
---

[Delayed tasks](../index.md#delayed-tasks) and [message expiration](../index.md#message-expiration) are two
independent features, but combined they give you a "retry after N seconds, but give up after a deadline" pattern
without any extra infrastructure — no scheduler, no separate retry queue.

```python
--8<-- "docs/examples/reliable_task_delivery.py"
```

!!! warning "Deadline must be greater than delay"
    `expiry` is an absolute unix timestamp computed once, at kick time. The message isn't even visible to a
    worker until `delay_seconds` has elapsed, so if `deadline_seconds <= delay_seconds`, the message will always
    be expired by the time anyone could receive it — it'll be silently discarded on delivery instead of running.
    Always leave a gap between the two: `deadline_seconds` should account for `delay_seconds` plus however long
    you're willing to let the message sit in the queue after it becomes visible.

To run it:

1. Start a worker:

    ```bash
    taskiq worker docs.examples.reliable_task_delivery:broker
    ```

2. In another terminal, schedule the task:

    ```bash
    python docs/examples/reliable_task_delivery.py
    ```

The worker won't pick up the message for about 5 seconds (the `delay`), and prints `Sent verification email to user
42` once it does. If you stop the worker for longer than the remaining deadline before restarting it, the message
is silently discarded instead — check the worker logs for `Discarding expired message from queue '...'`.
