using System.ComponentModel.DataAnnotations;

namespace KafkaConsumerLab.Worker.Configuration;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    [Required]
    public string BootstrapServers { get; init; } = "localhost:9092";

    [Required]
    public string GroupId { get; init; } = "dotnet-kafka-consumer-lab";

    [Required]
    public string InputTopic { get; init; } = "orders.created";

    [Required]
    public string RetryTopic { get; init; } = "orders.created.retry";

    [Required]
    public string DlqTopic { get; init; } = "orders.created.dlq";

    [Required]
    public string AutoOffsetReset { get; init; } = "Earliest";
}
