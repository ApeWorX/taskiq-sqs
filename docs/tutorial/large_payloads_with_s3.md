---
title: Large payloads with S3
---

SQS messages are capped at 256 KiB, which real workloads can outgrow quickly — a document to summarize, a batch of
rows to import, a rendered file to process. [`S3OffloadMiddleware`](../index.md#offloading-large-messages-to-s3)
transparently moves oversized payloads through S3 instead, and pairs naturally with
[`S3ResultBackend`](../index.md#basic-usage) for the result on the way back — both are just S3 buckets, configured
the same way.

```python
--8<-- "docs/examples/large_payloads_with_s3.py"
```

This example uses three separate resources: the queue itself, a bucket for offloaded payloads, and a bucket for
results. All three are declared automatically on `startup()`, same as in the basic example.

To run it:

1. Start a worker:

    ```bash
    taskiq worker docs.examples.large_payloads_with_s3:broker
    ```

2. In another terminal, run the script. It builds an ~1.1 MB string — well past the SQS limit — kicks it, and waits
   for the result:

    ```bash
    python docs/examples/large_payloads_with_s3.py
    ```

You should see something like `{'characters': 1100000, 'words': 100000}` printed once the worker finishes. Behind
the scenes: the middleware uploaded the document to `large-payload-offload` before the message ever reached SQS,
the worker downloaded it back before running `summarize_document`, deleted it from S3 afterwards (the default
`delete_after_execute=True`), and the result itself was written to `large-payload-results`.
