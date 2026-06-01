using System.ComponentModel.DataAnnotations;

namespace KafkaConsumerLab.Worker.Configuration;

public sealed class IdempotencyOptions
{
    public const string SectionName = "Idempotency";

    [Required]
    public string ConnectionString { get; init; } = "Data Source=consumer-state.db";
}
