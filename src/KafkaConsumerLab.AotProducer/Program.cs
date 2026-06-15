using System.Diagnostics;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaConsumerLab.Contracts;

var options = ProducerRunOptions.Parse(args);

if (options.CreateTopic)
{
    await CreateTopicAsync(options).ConfigureAwait(false);
}

var config = new ProducerConfig
{
    BootstrapServers = options.BootstrapServers,
    Acks = options.AcksAll ? Acks.All : Acks.Leader,
    EnableIdempotence = options.EnableIdempotence,
    LingerMs = options.LingerMs,
    BatchSize = options.BatchSize,
    MessageSendMaxRetries = 5
};

using var producer = new ProducerBuilder<string, string>(config).Build();

var delivered = 0;
var failed = 0;
var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(options.TimeoutSeconds));
await using var registration = timeout.Token.Register(() => completion.TrySetCanceled(timeout.Token));

var stopwatch = Stopwatch.StartNew();

for (var index = 0; index < options.Count; index++)
{
    var message = CreateMessage(index);
    var payload = JsonSerializer.Serialize(message, OrderCreatedJsonContext.Default.OrderCreated);

    producer.Produce(options.Topic, new Message<string, string>
    {
        Key = message.EventId,
        Value = payload
    }, report =>
    {
        if (report.Error.IsError)
        {
            Interlocked.Increment(ref failed);
        }
        else
        {
            Interlocked.Increment(ref delivered);
        }

        if (Volatile.Read(ref delivered) + Volatile.Read(ref failed) == options.Count)
        {
            completion.TrySetResult();
        }
    });
}

await completion.Task.ConfigureAwait(false);
producer.Flush(TimeSpan.FromSeconds(10));
stopwatch.Stop();

var perSecond = delivered / Math.Max(stopwatch.Elapsed.TotalSeconds, 0.001d);
Console.WriteLine("Produced={0} Failed={1} ElapsedMs={2:N0} RatePerSecond={3:N0}",
    delivered,
    failed,
    stopwatch.Elapsed.TotalMilliseconds,
    perSecond);

return failed == 0 ? 0 : 1;

static async Task CreateTopicAsync(ProducerRunOptions options)
{
    using var adminClient = new AdminClientBuilder(new AdminClientConfig
    {
        BootstrapServers = options.BootstrapServers
    }).Build();

    try
    {
        await adminClient.CreateTopicsAsync([
            new TopicSpecification
            {
                Name = options.Topic,
                NumPartitions = options.Partitions,
                ReplicationFactor = options.ReplicationFactor
            }
        ]).ConfigureAwait(false);
    }
    catch (CreateTopicsException exception)
        when (exception.Results.Any(result => result.Error.Code == ErrorCode.TopicAlreadyExists))
    {
    }
}

OrderCreated CreateMessage(int index)
{
    var suffix = $"{options.EventPrefix}-{index:D8}";

    return new OrderCreated(
        suffix,
        $"order-{index:D8}",
        $"customer-{index % 1000:D4}",
        42.50m,
        "USD",
        DateTimeOffset.UtcNow,
        null);
}

internal sealed record ProducerRunOptions(
    string BootstrapServers,
    string Topic,
    int Count,
    string EventPrefix,
    bool AcksAll,
    bool EnableIdempotence,
    int LingerMs,
    int BatchSize,
    int TimeoutSeconds,
    bool CreateTopic,
    int Partitions,
    short ReplicationFactor)
{
    public static ProducerRunOptions Parse(string[] args) =>
        new(
            ReadString(args, "--bootstrap", "localhost:9092"),
            ReadString(args, "--topic", "orders.created"),
            ReadInt(args, "--count", 10000),
            ReadString(args, "--event-prefix", $"aot-{DateTimeOffset.UtcNow:yyyyMMddHHmmss}"),
            ReadBool(args, "--acks-all", true),
            ReadBool(args, "--idempotence", true),
            ReadInt(args, "--linger-ms", 5),
            ReadInt(args, "--batch-size", 1000000),
            ReadInt(args, "--timeout-seconds", 60),
            ReadBool(args, "--create-topic", false),
            Math.Max(1, ReadInt(args, "--partitions", 1)),
            (short)Math.Max(1, ReadInt(args, "--replication-factor", 1)));

    private static string ReadString(string[] args, string name, string defaultValue)
    {
        var index = Array.IndexOf(args, name);
        return index >= 0 && index < args.Length - 1 ? args[index + 1] : defaultValue;
    }

    private static int ReadInt(string[] args, string name, int defaultValue) =>
        int.TryParse(ReadString(args, name, string.Empty), out var value) ? value : defaultValue;

    private static bool ReadBool(string[] args, string name, bool defaultValue) =>
        bool.TryParse(ReadString(args, name, string.Empty), out var value) ? value : defaultValue;
}
