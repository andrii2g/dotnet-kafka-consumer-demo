using KafkaConsumerLab.Contracts;

namespace KafkaConsumerLab.Worker.Idempotency;

public static class IdempotencyKey
{
    public static string Create(OrderCreated message)
    {
        if (string.IsNullOrWhiteSpace(message.EventId))
        {
            throw new ArgumentException("EventId is required for idempotency.", nameof(message));
        }

        return $"OrderCreated:{message.EventId}";
    }
}
