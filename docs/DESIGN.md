# Design

## Commit Model

- Auto commit is disabled so the worker controls when offsets advance.
- Processing happens before commit, which gives at-least-once behavior.
- Retry and DLQ publishes happen before commit so failed messages are not silently lost.

## Idempotency

- The worker uses `OrderCreated:{EventId}` as the idempotency key.
- The SQLite table is written only after successful domain processing.
- If Kafka redelivers after a commit failure or crash, the duplicate is skipped safely.

## Retry

- V1 uses `orders.created.retry` as a parking topic.
- `x-attempt` tracks total processing attempts.
- `x-next-delay-ms` is informational only in V1.

## Threading

- V1 processes one message at a time.
- This keeps offset handling simple and avoids partition ordering bugs.
