namespace KafkaConsumerLab.Worker.Diagnostics;

public static class LogEvents
{
    public const string ConsumerStarted = nameof(ConsumerStarted);
    public const string ConsumerStopped = nameof(ConsumerStopped);
    public const string MessageReceived = nameof(MessageReceived);
    public const string MessageProcessed = nameof(MessageProcessed);
    public const string MessageDuplicateSkipped = nameof(MessageDuplicateSkipped);
    public const string MessageRetried = nameof(MessageRetried);
    public const string MessageDeadLettered = nameof(MessageDeadLettered);
    public const string OffsetCommitted = nameof(OffsetCommitted);
    public const string ConsumeError = nameof(ConsumeError);
    public const string CommitFailed = nameof(CommitFailed);
    public const string ProcessingFailed = nameof(ProcessingFailed);
    public const string DlqPublishFailed = nameof(DlqPublishFailed);
    public const string RetryPublishFailed = nameof(RetryPublishFailed);
    public const string PartitionsAssigned = nameof(PartitionsAssigned);
    public const string PartitionsRevoked = nameof(PartitionsRevoked);
}
