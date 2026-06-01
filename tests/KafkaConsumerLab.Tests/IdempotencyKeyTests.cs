using FluentAssertions;
using KafkaConsumerLab.Contracts;
using KafkaConsumerLab.Worker.Idempotency;

namespace KafkaConsumerLab.Tests;

public sealed class IdempotencyKeyTests
{
    [Fact]
    public void Valid_event_id_produces_expected_key()
    {
        var message = CreateMessage("evt-123");

        IdempotencyKey.Create(message).Should().Be("OrderCreated:evt-123");
    }

    [Fact]
    public void Null_event_id_is_invalid()
    {
        var action = () => IdempotencyKey.Create(CreateMessage(null!));

        action.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Whitespace_event_id_is_invalid()
    {
        var action = () => IdempotencyKey.Create(CreateMessage("  "));

        action.Should().Throw<ArgumentException>();
    }

    private static OrderCreated CreateMessage(string eventId) =>
        new(
            eventId,
            "order-1",
            "customer-1",
            10m,
            "USD",
            DateTimeOffset.UtcNow,
            null);
}
