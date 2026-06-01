using KafkaConsumerLab.Contracts;

namespace KafkaConsumerLab.Worker.Processing;

public sealed class OrderMessageProcessor : IOrderMessageProcessor
{
    public async Task ProcessAsync(OrderCreated message, CancellationToken cancellationToken)
    {
        switch (message.FailureMode?.Trim().ToLowerInvariant())
        {
            case null:
            case "":
                return;
            case "transient":
                throw new TransientProcessingException("Simulated transient failure.");
            case "poison":
                throw new PoisonMessageException("Simulated poison message.");
            case "unexpected":
                throw new InvalidOperationException("Simulated unexpected failure.");
            case "slow":
                await Task.Delay(TimeSpan.FromSeconds(10), cancellationToken).ConfigureAwait(false);
                return;
            default:
                return;
        }
    }
}
