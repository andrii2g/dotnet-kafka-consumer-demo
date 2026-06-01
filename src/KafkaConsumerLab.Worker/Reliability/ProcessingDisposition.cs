namespace KafkaConsumerLab.Worker.Reliability;

public enum ProcessingDisposition
{
    Shutdown = 0,
    Transient = 1,
    Poison = 2,
    Unexpected = 3
}
