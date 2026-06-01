using System.Text.Json;
using KafkaConsumerLab.Contracts;
using KafkaConsumerLab.Worker.Reliability;

namespace KafkaConsumerLab.Worker.Kafka;

public sealed class KafkaMessageSerializer
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    public OrderCreated DeserializeOrderCreated(string json) =>
        JsonSerializer.Deserialize<OrderCreated>(json, SerializerOptions)
        ?? throw new JsonException("Payload deserialized to null.");

    public string SerializeOrderCreated(OrderCreated message) =>
        JsonSerializer.Serialize(message, SerializerOptions);

    public string SerializeDlqEnvelope(DlqEnvelope envelope) =>
        JsonSerializer.Serialize(envelope, SerializerOptions);
}
