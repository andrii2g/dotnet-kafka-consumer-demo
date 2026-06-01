namespace KafkaConsumerLab.Worker.Kafka;

public sealed class KafkaConsumerHostedService(KafkaConsumerRunner runner) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken) => runner.RunAsync(stoppingToken);
}
