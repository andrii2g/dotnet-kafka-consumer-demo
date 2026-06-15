using System.Text.Json.Serialization;

namespace KafkaConsumerLab.Contracts;

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(OrderCreated))]
public sealed partial class OrderCreatedJsonContext : JsonSerializerContext;
