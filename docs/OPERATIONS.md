# Operations

## Inspect Topics

- Open Kafka UI at `http://localhost:8080`.
- Inspect `orders.created`, `orders.created.retry`, and `orders.created.dlq`.

## Reset Local State

- Stop the worker.
- Delete `consumer-state.db`.
- If needed, reset the consumer group offsets from Kafka UI or with Kafka CLI tools inside the broker container.

## Replay DLQ Messages

- Copy the original payload from the DLQ envelope and produce it back to `orders.created`.
- Preserve the key when replaying.

## Scale

- Multiple worker instances can share the same group id.
- Parallelism is limited by partition count.
- SQLite is local, so this sample should not be treated as a production multi-instance idempotency design.
