using Confluent.Kafka;
using KafkaConsumerLab.Contracts;
using KafkaConsumerLab.Worker.Configuration;
using KafkaConsumerLab.Worker.Diagnostics;
using KafkaConsumerLab.Worker.Idempotency;
using KafkaConsumerLab.Worker.Processing;
using KafkaConsumerLab.Worker.Reliability;
using Microsoft.Extensions.Options;

namespace KafkaConsumerLab.Worker.Kafka;

public sealed class KafkaConsumerRunner(
    KafkaConsumerFactory consumerFactory,
    KafkaProducerFactory producerFactory,
    IOptions<KafkaOptions> kafkaOptions,
    IOptions<RetryOptions> retryOptions,
    KafkaMessageSerializer serializer,
    IOrderMessageProcessor processor,
    IProcessedMessageStore processedMessageStore,
    ProcessingAttemptClassifier attemptClassifier,
    RetryPublisher retryPublisher,
    DlqPublisher dlqPublisher,
    ConsumerMetrics metrics,
    ILogger<KafkaConsumerRunner> logger) : IDisposable
{
    private IProducer<string, string>? _producer;

    public async Task RunAsync(CancellationToken stoppingToken)
    {
        _producer = producerFactory.CreateProducer();

        using var consumer = consumerFactory.CreateConsumer(OnAssigned, OnRevoked, OnLost);
        consumer.Subscribe(kafkaOptions.Value.InputTopic);

        logger.LogInformation("{EventName} {ConsumerGroup}", LogEvents.ConsumerStarted, kafkaOptions.Value.GroupId);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string>? result = null;

                try
                {
                    result = consumer.Consume(stoppingToken);
                    if (result is null)
                    {
                        continue;
                    }

                    metrics.Increment("MessagesReceived");
                    LogMessage(LogEvents.MessageReceived, result, eventId: null);

                    var decision = await ProcessOneAsync(result, stoppingToken).ConfigureAwait(false);

                    consumer.Commit(result);
                    logger.LogInformation(
                        "{EventName} topic={Topic} partition={Partition} offset={Offset} key={Key} outcome={Outcome}",
                        LogEvents.OffsetCommitted,
                        result.Topic,
                        result.Partition.Value,
                        result.Offset.Value,
                        result.Message.Key,
                        decision);
                }
                catch (ConsumeException exception)
                {
                    logger.LogError(exception, "{EventName} Kafka consume error", LogEvents.ConsumeError);
                }
                catch (KafkaException exception) when (result is not null && exception.Error.Code == ErrorCode.Local_State)
                {
                    logger.LogError(exception, "{EventName} commit failed for topic={Topic} partition={Partition} offset={Offset}", LogEvents.CommitFailed, result.Topic, result.Partition.Value, result.Offset.Value);
                    metrics.Increment("CommitFailures");
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception exception)
                {
                    logger.LogError(exception, "{EventName} unexpected consumer loop error", LogEvents.ProcessingFailed);
                    metrics.Increment("ProcessingFailures");
                }
            }
        }
        finally
        {
            consumer.Close();
            _producer.Flush(TimeSpan.FromSeconds(10));
            logger.LogInformation("{EventName} {ConsumerGroup}", LogEvents.ConsumerStopped, kafkaOptions.Value.GroupId);
        }
    }

    private async Task<ProcessingResult> ProcessOneAsync(ConsumeResult<string, string> result, CancellationToken cancellationToken)
    {
        var rawPayload = result.Message.Value;
        var eventId = default(string);

        try
        {
            var message = serializer.DeserializeOrderCreated(rawPayload);
            Validate(message);
            eventId = message.EventId;

            var idempotencyKey = IdempotencyKey.Create(message);
            if (await processedMessageStore.ExistsAsync(idempotencyKey, cancellationToken).ConfigureAwait(false))
            {
                metrics.Increment("MessagesDuplicated");
                LogMessage(LogEvents.MessageDuplicateSkipped, result, eventId);
                return ProcessingResult.Duplicate;
            }

            await processor.ProcessAsync(message, cancellationToken).ConfigureAwait(false);

            if (!await processedMessageStore.TryMarkProcessedAsync(idempotencyKey, result.Topic, result.Partition.Value, result.Offset.Value, cancellationToken).ConfigureAwait(false))
            {
                metrics.Increment("MessagesDuplicated");
                LogMessage(LogEvents.MessageDuplicateSkipped, result, eventId);
                return ProcessingResult.Duplicate;
            }

            metrics.Increment("MessagesProcessed");
            LogMessage(LogEvents.MessageProcessed, result, eventId);
            return ProcessingResult.Processed;
        }
        catch (Exception exception) when (exception is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            var classification = attemptClassifier.Classify(exception);
            metrics.Increment("ProcessingFailures");

            switch (classification)
            {
                case ProcessingDisposition.Transient:
                {
                    var attemptState = attemptClassifier.ReadAttemptState(result.Message.Headers, kafkaOptions.Value.GroupId, logger);

                    if (attemptState.CurrentAttempt < retryOptions.Value.MaxAttempts)
                    {
                        await retryPublisher.PublishAsync(_producer!, result, rawPayload, exception, attemptState, cancellationToken).ConfigureAwait(false);
                        metrics.Increment("MessagesRetried");
                        LogMessage(LogEvents.MessageRetried, result, eventId);
                        return ProcessingResult.Retried;
                    }

                    await dlqPublisher.PublishAsync(_producer!, result, rawPayload, exception, cancellationToken).ConfigureAwait(false);
                    metrics.Increment("MessagesDeadLettered");
                    LogMessage(LogEvents.MessageDeadLettered, result, eventId);
                    return ProcessingResult.DeadLettered;
                }

                case ProcessingDisposition.Poison:
                case ProcessingDisposition.Unexpected:
                {
                    await dlqPublisher.PublishAsync(_producer!, result, rawPayload, exception, cancellationToken).ConfigureAwait(false);
                    metrics.Increment("MessagesDeadLettered");
                    LogMessage(LogEvents.MessageDeadLettered, result, eventId);
                    return ProcessingResult.DeadLettered;
                }

                default:
                    throw;
            }
        }
    }

    private void Validate(OrderCreated message)
    {
        if (string.IsNullOrWhiteSpace(message.EventId))
        {
            throw new PoisonMessageException("EventId is required.");
        }

        if (string.IsNullOrWhiteSpace(message.OrderId))
        {
            throw new PoisonMessageException("OrderId is required.");
        }

        if (string.IsNullOrWhiteSpace(message.CustomerId))
        {
            throw new PoisonMessageException("CustomerId is required.");
        }
    }

    private void LogMessage(string eventName, ConsumeResult<string, string> result, string? eventId)
    {
        logger.LogInformation(
            "{EventName} topic={Topic} partition={Partition} offset={Offset} key={Key} eventId={EventId} consumerGroup={ConsumerGroup}",
            eventName,
            result.Topic,
            result.Partition.Value,
            result.Offset.Value,
            result.Message.Key,
            eventId,
            kafkaOptions.Value.GroupId);
    }

    private void OnAssigned(IConsumer<string, string> _, List<TopicPartition> partitions) =>
        logger.LogInformation("{EventName} {Partitions}", LogEvents.PartitionsAssigned, string.Join(", ", partitions));

    private void OnRevoked(IConsumer<string, string> _, List<TopicPartitionOffset> partitions) =>
        logger.LogInformation("{EventName} {Partitions}", LogEvents.PartitionsRevoked, string.Join(", ", partitions));

    private void OnLost(IConsumer<string, string> _, List<TopicPartitionOffset> partitions) =>
        logger.LogWarning("{EventName} {Partitions}", LogEvents.PartitionsRevoked, string.Join(", ", partitions));

    public void Dispose() => _producer?.Dispose();
}
