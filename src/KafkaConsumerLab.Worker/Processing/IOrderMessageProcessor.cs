using KafkaConsumerLab.Contracts;

namespace KafkaConsumerLab.Worker.Processing;

public interface IOrderMessageProcessor
{
    Task ProcessAsync(OrderCreated message, CancellationToken cancellationToken);
}
