using Confluent.Kafka;
using KafkaConsumerLab.Worker.Configuration;
using KafkaConsumerLab.Worker.Kafka;
using Microsoft.Extensions.Options;

namespace KafkaConsumerLab.Worker.Reliability;

public sealed class RetryPublisher(
    IOptions<KafkaOptions> kafkaOptions,
    BackoffCalculator backoffCalculator,
    ILogger<RetryPublisher> logger)
{
    public async Task PublishAsync(
        IProducer<string, string> producer,
        ConsumeResult<string, string> source,
        string rawPayload,
        Exception exception,
        AttemptState state,
        CancellationToken cancellationToken)
    {
        var nextAttempt = state.CurrentAttempt + 1;
        var now = DateTimeOffset.UtcNow;
        var headers = KafkaHeaders.Clone(source.Message.Headers ?? []);
        KafkaHeaders.SetString(headers, KafkaHeaders.OriginalTopic, source.Topic);
        KafkaHeaders.SetString(headers, KafkaHeaders.OriginalPartition, source.Partition.Value.ToString());
        KafkaHeaders.SetString(headers, KafkaHeaders.OriginalOffset, source.Offset.Value.ToString());
        KafkaHeaders.SetString(headers, KafkaHeaders.OriginalKey, source.Message.Key);
        KafkaHeaders.SetString(headers, KafkaHeaders.ErrorType, exception.GetType().Name);
        KafkaHeaders.SetString(headers, KafkaHeaders.ErrorMessage, exception.Message);
        KafkaHeaders.SetString(headers, KafkaHeaders.Attempt, nextAttempt.ToString());
        KafkaHeaders.SetString(headers, KafkaHeaders.FirstFailedAtUtc, state.FirstFailedAtUtc.ToString("O"));
        KafkaHeaders.SetString(headers, KafkaHeaders.LastFailedAtUtc, now.ToString("O"));
        KafkaHeaders.SetString(headers, KafkaHeaders.NextDelayMs, backoffCalculator.CalculateDelayMs(nextAttempt).ToString());

        var message = new Message<string, string>
        {
            Key = source.Message.Key,
            Value = rawPayload,
            Headers = headers
        };

        await producer.ProduceAsync(kafkaOptions.Value.RetryTopic, message, cancellationToken).ConfigureAwait(false);
        logger.LogInformation("{EventName} published retry message for key={Key}", Diagnostics.LogEvents.MessageRetried, source.Message.Key);
    }
}
