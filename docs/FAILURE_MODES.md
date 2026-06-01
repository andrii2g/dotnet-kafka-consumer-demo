# Failure Modes

## Worker crashes before processing

- Expected behavior: Kafka redelivers the message.
- Redelivery possible: yes.
- Message loss possible: no.
- Expected logs: `MessageReceived` may appear without `OffsetCommitted`.

## Worker crashes after processing but before commit

- Expected behavior: Kafka may redeliver and SQLite idempotency converts the second delivery into `MessageDuplicateSkipped`.
- Redelivery possible: yes.
- Message loss possible: no.
- Expected logs: `MessageProcessed` without `OffsetCommitted`.

## Worker crashes after retry publish but before commit

- Expected behavior: the source message may redeliver and may publish to retry again.
- Redelivery possible: yes.
- Message loss possible: no.
- Expected logs: `MessageRetried` without `OffsetCommitted`.

## Worker crashes after DLQ publish but before commit

- Expected behavior: the source message may redeliver and may publish to DLQ again.
- Redelivery possible: yes.
- Message loss possible: no.
- Expected logs: `MessageDeadLettered` without `OffsetCommitted`.

## Invalid JSON or missing EventId

- Expected behavior: the worker writes a DLQ envelope and commits only after the DLQ publish succeeds.
- Redelivery possible: only if DLQ publish or commit fails.
- Message loss possible: no.
- Expected logs: `MessageDeadLettered`.
