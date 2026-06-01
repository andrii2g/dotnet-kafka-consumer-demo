namespace KafkaConsumerLab.Worker.Processing;

public sealed class PoisonMessageException(string message) : Exception(message);
