namespace KafkaConsumerLab.Worker.Reliability;

public sealed record AttemptState(int CurrentAttempt, DateTimeOffset FirstFailedAtUtc);
