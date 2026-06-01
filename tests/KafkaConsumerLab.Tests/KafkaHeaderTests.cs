using Confluent.Kafka;
using FluentAssertions;
using KafkaConsumerLab.Worker.Kafka;

namespace KafkaConsumerLab.Tests;

public sealed class KafkaHeaderTests
{
    [Fact]
    public void Can_add_and_read_utf8_header()
    {
        var headers = new Headers();
        KafkaHeaders.SetString(headers, KafkaHeaders.OriginalTopic, "orders.created");

        KafkaHeaders.GetString(headers, KafkaHeaders.OriginalTopic).Should().Be("orders.created");
    }

    [Fact]
    public void Missing_header_returns_null()
    {
        KafkaHeaders.GetString(new Headers(), "missing").Should().BeNull();
    }

    [Fact]
    public void Invalid_utf8_does_not_throw()
    {
        var headers = new Headers
        {
            { "bad", new byte[] { 0xC3, 0x28 } }
        };

        var action = () => KafkaHeaders.GetString(headers, "bad");

        action.Should().NotThrow();
    }
}
