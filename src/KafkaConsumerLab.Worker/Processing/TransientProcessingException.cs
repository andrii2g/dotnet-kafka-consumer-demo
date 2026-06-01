namespace KafkaConsumerLab.Worker.Processing;

public sealed class TransientProcessingException(string message) : Exception(message);
