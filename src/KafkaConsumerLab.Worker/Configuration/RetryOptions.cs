using System.ComponentModel.DataAnnotations;

namespace KafkaConsumerLab.Worker.Configuration;

public sealed class RetryOptions
{
    public const string SectionName = "Retry";

    [Range(1, int.MaxValue)]
    public int MaxAttempts { get; init; } = 3;

    [Range(0, int.MaxValue)]
    public int BaseDelayMs { get; init; } = 250;

    [Range(0, int.MaxValue)]
    public int MaxDelayMs { get; init; } = 5000;
}
