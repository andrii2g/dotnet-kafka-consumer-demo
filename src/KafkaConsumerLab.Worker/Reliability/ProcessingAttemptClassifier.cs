using System.Text.Json;
using Confluent.Kafka;
using KafkaConsumerLab.Worker.Kafka;
using KafkaConsumerLab.Worker.Processing;

namespace KafkaConsumerLab.Worker.Reliability;

public sealed class ProcessingAttemptClassifier
{
    public ProcessingDisposition Classify(Exception exception) =>
        exception switch
        {
            JsonException => ProcessingDisposition.Poison,
            PoisonMessageException => ProcessingDisposition.Poison,
            TransientProcessingException => ProcessingDisposition.Transient,
            OperationCanceledException => ProcessingDisposition.Shutdown,
            _ => ProcessingDisposition.Unexpected
        };

    public AttemptState ReadAttemptState(Headers headers, string consumerGroup, ILogger logger)
    {
        var attemptValue = KafkaHeaders.GetString(headers, KafkaHeaders.Attempt);
        var parsedAttempt = 1;

        if (!string.IsNullOrWhiteSpace(attemptValue) && !int.TryParse(attemptValue, out parsedAttempt))
        {
            parsedAttempt = 1;
            logger.LogWarning("{EventName} invalid {Header} value for consumerGroup={ConsumerGroup}", "RetryHeaderInvalid", KafkaHeaders.Attempt, consumerGroup);
        }

        if (parsedAttempt < 1)
        {
            parsedAttempt = 1;
        }

        var firstFailedAt = DateTimeOffset.UtcNow;
        var firstFailedAtValue = KafkaHeaders.GetString(headers, KafkaHeaders.FirstFailedAtUtc);
        if (!string.IsNullOrWhiteSpace(firstFailedAtValue) && DateTimeOffset.TryParse(firstFailedAtValue, out var parsedFirstFailedAt))
        {
            firstFailedAt = parsedFirstFailedAt;
        }

        return new AttemptState(parsedAttempt, firstFailedAt);
    }
}
