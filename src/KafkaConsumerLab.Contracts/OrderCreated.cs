namespace KafkaConsumerLab.Contracts;

public sealed record OrderCreated(
    string EventId,
    string OrderId,
    string CustomerId,
    decimal Amount,
    string Currency,
    DateTimeOffset CreatedAtUtc,
    string? FailureMode);
