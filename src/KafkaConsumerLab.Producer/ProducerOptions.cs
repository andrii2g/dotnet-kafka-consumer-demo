namespace KafkaConsumerLab.Producer;

public sealed class ProducerOptions
{
    public string BootstrapServers { get; init; } = "localhost:9092";

    public string Topic { get; init; } = "orders.created";
}
