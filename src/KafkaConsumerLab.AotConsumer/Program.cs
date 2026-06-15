using System.Diagnostics;
using System.Text.Json;
using Confluent.Kafka;
using KafkaConsumerLab.Contracts;

var options = ConsumerRunOptions.Parse(args);

var config = new ConsumerConfig
{
    BootstrapServers = options.BootstrapServers,
    GroupId = options.GroupId,
    EnableAutoCommit = false,
    EnableAutoOffsetStore = false,
    AutoOffsetReset = options.FromBeginning ? AutoOffsetReset.Earliest : AutoOffsetReset.Latest,
    PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
    MaxPollIntervalMs = 300000,
    SessionTimeoutMs = 45000,
    EnablePartitionEof = false
};

using var consumer = new ConsumerBuilder<string, string>(config).Build();
using var shutdown = new CancellationTokenSource();
Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    shutdown.Cancel();
};

consumer.Subscribe(options.Topic);

var consumed = 0;
var deserialized = 0;
var stopwatch = Stopwatch.StartNew();

try
{
    while (!shutdown.IsCancellationRequested && consumed < options.Count)
    {
        var result = consumer.Consume(TimeSpan.FromMilliseconds(options.PollTimeoutMs));
        if (result is null)
        {
            continue;
        }

        if (options.Deserialize)
        {
            _ = JsonSerializer.Deserialize(result.Message.Value, OrderCreatedJsonContext.Default.OrderCreated)
                ?? throw new JsonException("Payload deserialized to null.");
            deserialized++;
        }

        consumed++;
        consumer.StoreOffset(result);

        if (consumed % options.CommitBatch == 0)
        {
            consumer.Commit();
        }
    }

    if (consumed > 0)
    {
        consumer.Commit();
    }
}
finally
{
    consumer.Close();
    stopwatch.Stop();
}

var perSecond = consumed / Math.Max(stopwatch.Elapsed.TotalSeconds, 0.001d);
Console.WriteLine("Consumed={0} Deserialized={1} ElapsedMs={2:N0} RatePerSecond={3:N0}",
    consumed,
    deserialized,
    stopwatch.Elapsed.TotalMilliseconds,
    perSecond);

return consumed >= options.Count ? 0 : 2;

internal sealed record ConsumerRunOptions(
    string BootstrapServers,
    string Topic,
    string GroupId,
    int Count,
    int CommitBatch,
    bool FromBeginning,
    bool Deserialize,
    int PollTimeoutMs)
{
    public static ConsumerRunOptions Parse(string[] args) =>
        new(
            ReadString(args, "--bootstrap", "localhost:9092"),
            ReadString(args, "--topic", "orders.created"),
            ReadString(args, "--group-id", $"aot-consumer-{Environment.MachineName}"),
            ReadInt(args, "--count", 10000),
            Math.Max(1, ReadInt(args, "--commit-batch", 1000)),
            ReadBool(args, "--from-beginning", true),
            ReadBool(args, "--deserialize", true),
            ReadInt(args, "--poll-timeout-ms", 100));

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
