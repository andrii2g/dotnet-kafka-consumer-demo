namespace KafkaConsumerLab.Contracts;

public enum ProcessingResult
{
    Processed = 0,
    Duplicate = 1,
    Retried = 2,
    DeadLettered = 3
}
