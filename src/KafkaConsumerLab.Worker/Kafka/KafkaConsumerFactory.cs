using Confluent.Kafka;
using KafkaConsumerLab.Worker.Configuration;
using Microsoft.Extensions.Options;

namespace KafkaConsumerLab.Worker.Kafka;

public sealed class KafkaConsumerFactory(IOptions<KafkaOptions> options)
{
    public IConsumer<string, string> CreateConsumer(
        Action<IConsumer<string, string>, List<TopicPartition>> onAssigned,
        Action<IConsumer<string, string>, List<TopicPartitionOffset>> onRevoked,
        Action<IConsumer<string, string>, List<TopicPartitionOffset>> onLost)
    {
        var kafkaOptions = options.Value;
        var config = new ConsumerConfig
        {
            BootstrapServers = kafkaOptions.BootstrapServers,
            GroupId = kafkaOptions.GroupId,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = ParseOffsetReset(kafkaOptions.AutoOffsetReset),
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
            MaxPollIntervalMs = 300000,
            SessionTimeoutMs = 45000,
            EnablePartitionEof = false
        };

        return new ConsumerBuilder<string, string>(config)
            .SetPartitionsAssignedHandler(onAssigned)
            .SetPartitionsRevokedHandler(onRevoked)
            .SetPartitionsLostHandler(onLost)
            .Build();
    }

    private static AutoOffsetReset ParseOffsetReset(string value) =>
        value.Equals("Earliest", StringComparison.OrdinalIgnoreCase)
            ? AutoOffsetReset.Earliest
            : AutoOffsetReset.Latest;
}
