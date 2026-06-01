namespace KafkaConsumerLab.Worker.Reliability;

public sealed record DlqEnvelope(
    string? OriginalKey,
    string OriginalTopic,
    int OriginalPartition,
    long OriginalOffset,
    DateTimeOffset FailedAtUtc,
    string ErrorType,
    string ErrorMessage,
    string Payload);
