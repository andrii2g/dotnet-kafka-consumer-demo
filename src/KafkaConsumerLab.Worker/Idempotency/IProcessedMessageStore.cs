namespace KafkaConsumerLab.Worker.Idempotency;

public interface IProcessedMessageStore
{
    Task InitializeAsync(CancellationToken cancellationToken);

    Task<bool> ExistsAsync(string idempotencyKey, CancellationToken cancellationToken);

    Task<bool> TryMarkProcessedAsync(string idempotencyKey, string topic, int partition, long offset, CancellationToken cancellationToken);
}
