using Confluent.Kafka;
using KafkaConsumerLab.Worker.Configuration;
using KafkaConsumerLab.Worker.Kafka;
using Microsoft.Extensions.Options;

namespace KafkaConsumerLab.Worker.Reliability;

public sealed class DlqPublisher(
    IOptions<KafkaOptions> kafkaOptions,
    KafkaMessageSerializer serializer)
{
    public async Task PublishAsync(
        IProducer<string, string> producer,
        ConsumeResult<string, string> source,
        string rawPayload,
        Exception exception,
        CancellationToken cancellationToken)
    {
        var envelope = new DlqEnvelope(
            source.Message.Key,
            source.Topic,
            source.Partition.Value,
            source.Offset.Value,
            DateTimeOffset.UtcNow,
            exception.GetType().Name,
            exception.Message,
            rawPayload);

        var headers = KafkaHeaders.Clone(source.Message.Headers ?? []);
        KafkaHeaders.SetString(headers, KafkaHeaders.OriginalTopic, envelope.OriginalTopic);
        KafkaHeaders.SetString(headers, KafkaHeaders.OriginalPartition, envelope.OriginalPartition.ToString());
        KafkaHeaders.SetString(headers, KafkaHeaders.OriginalOffset, envelope.OriginalOffset.ToString());
        KafkaHeaders.SetString(headers, KafkaHeaders.OriginalKey, envelope.OriginalKey);
        KafkaHeaders.SetString(headers, KafkaHeaders.ErrorType, envelope.ErrorType);
        KafkaHeaders.SetString(headers, KafkaHeaders.ErrorMessage, envelope.ErrorMessage);
        KafkaHeaders.SetString(headers, KafkaHeaders.LastFailedAtUtc, envelope.FailedAtUtc.ToString("O"));

        var message = new Message<string, string>
        {
            Key = source.Message.Key,
            Value = serializer.SerializeDlqEnvelope(envelope),
            Headers = headers
        };

        await producer.ProduceAsync(kafkaOptions.Value.DlqTopic, message, cancellationToken).ConfigureAwait(false);
    }
}
