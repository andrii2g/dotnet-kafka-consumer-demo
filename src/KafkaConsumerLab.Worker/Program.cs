using KafkaConsumerLab.Worker.Configuration;
using KafkaConsumerLab.Worker.Diagnostics;
using KafkaConsumerLab.Worker.Idempotency;
using KafkaConsumerLab.Worker.Kafka;
using KafkaConsumerLab.Worker.Processing;
using KafkaConsumerLab.Worker.Reliability;
using Microsoft.Extensions.Options;

var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddOptions<KafkaOptions>()
    .Bind(builder.Configuration.GetSection(KafkaOptions.SectionName))
    .ValidateDataAnnotations()
    .ValidateOnStart();

builder.Services
    .AddOptions<RetryOptions>()
    .Bind(builder.Configuration.GetSection(RetryOptions.SectionName))
    .ValidateDataAnnotations()
    .Validate(static options => options.MaxDelayMs >= options.BaseDelayMs, "MaxDelayMs must be greater than or equal to BaseDelayMs.")
    .ValidateOnStart();

builder.Services
    .AddOptions<IdempotencyOptions>()
    .Bind(builder.Configuration.GetSection(IdempotencyOptions.SectionName))
    .ValidateDataAnnotations()
    .ValidateOnStart();

builder.Services.AddSingleton<ConsumerMetrics>();
builder.Services.AddSingleton<KafkaMessageSerializer>();
builder.Services.AddSingleton<KafkaConsumerFactory>();
builder.Services.AddSingleton<KafkaProducerFactory>();
builder.Services.AddSingleton<ProcessingAttemptClassifier>();
builder.Services.AddSingleton<BackoffCalculator>();
builder.Services.AddSingleton<RetryPublisher>();
builder.Services.AddSingleton<DlqPublisher>();
builder.Services.AddSingleton<IOrderMessageProcessor, OrderMessageProcessor>();
builder.Services.AddSingleton<IProcessedMessageStore, SqliteProcessedMessageStore>();
builder.Services.AddSingleton<KafkaConsumerRunner>();
builder.Services.AddHostedService<KafkaConsumerHostedService>();
builder.Services.AddHostedService<MetricsReporterHostedService>();

var host = builder.Build();

using (var scope = host.Services.CreateScope())
{
    var store = scope.ServiceProvider.GetRequiredService<IProcessedMessageStore>();
    var cancellationSource = new CancellationTokenSource(TimeSpan.FromSeconds(15));
    await store.InitializeAsync(cancellationSource.Token);
}

await host.RunAsync();
