namespace KafkaConsumerLab.Worker.Diagnostics;

public sealed class MetricsReporterHostedService(
    ConsumerMetrics metrics,
    ILogger<MetricsReporterHostedService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(30));

        while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
        {
            logger.LogInformation("MetricsSnapshot {Metrics}", metrics.Snapshot());
        }
    }
}
