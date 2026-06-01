using Confluent.Kafka;
using KafkaConsumerLab.Contracts;
using System.Text.Json;

var scenario = args.FirstOrDefault() ?? "valid";
var count = ReadIntOption(args, "--count", 1);
var fixedEventId = ReadStringOption(args, "--event-id");
var serializerOptions = new JsonSerializerOptions
{
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
};

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    Acks = Acks.All,
    EnableIdempotence = true,
    MessageSendMaxRetries = 5,
    LingerMs = 5
};

using var producer = new ProducerBuilder<string, string>(config).Build();

for (var index = 0; index < count; index++)
{
    var message = CreateMessage(scenario, index, fixedEventId);
    var payload = JsonSerializer.Serialize(message, serializerOptions);

    await producer.ProduceAsync("orders.created", new Message<string, string>
    {
        Key = message.EventId,
        Value = payload
    }).ConfigureAwait(false);

    Console.WriteLine("Produced key={0} failureMode={1}", message.EventId, message.FailureMode ?? "<none>");
}

producer.Flush(TimeSpan.FromSeconds(10));

return;

static OrderCreated CreateMessage(string scenario, int index, string? fixedEventId)
{
    var suffix = $"{DateTimeOffset.UtcNow:yyyyMMddHHmmss}-{index}";

    return scenario.ToLowerInvariant() switch
    {
        "valid" => NewMessage($"evt-{suffix}", null),
        "transient" => NewMessage($"evt-{suffix}", "transient"),
        "poison" => NewMessage($"evt-{suffix}", "poison"),
        "duplicate" => NewMessage(fixedEventId ?? "fixed-123", null),
        "mixed" => (index % 4) switch
        {
            0 => NewMessage($"evt-{suffix}", null),
            1 => NewMessage($"evt-{suffix}", "transient"),
            2 => NewMessage($"evt-{suffix}", "poison"),
            _ => NewMessage($"evt-{suffix}", "unexpected")
        },
        _ => NewMessage($"evt-{suffix}", null)
    };
}

static OrderCreated NewMessage(string eventId, string? failureMode) =>
    new(
        eventId,
        $"order-{Guid.NewGuid():N}"[..12],
        $"customer-{Guid.NewGuid():N}"[..12],
        42.50m,
        "USD",
        DateTimeOffset.UtcNow,
        failureMode);

static int ReadIntOption(string[] args, string optionName, int defaultValue)
{
    var raw = ReadStringOption(args, optionName);
    return raw is not null && int.TryParse(raw, out var parsed) ? parsed : defaultValue;
}

static string? ReadStringOption(string[] args, string optionName)
{
    var index = Array.IndexOf(args, optionName);
    return index >= 0 && index < args.Length - 1 ? args[index + 1] : null;
}
