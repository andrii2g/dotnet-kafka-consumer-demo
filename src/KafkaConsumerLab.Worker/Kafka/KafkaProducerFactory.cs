using Confluent.Kafka;
using KafkaConsumerLab.Worker.Configuration;
using Microsoft.Extensions.Options;

namespace KafkaConsumerLab.Worker.Kafka;

public sealed class KafkaProducerFactory(IOptions<KafkaOptions> options)
{
    public IProducer<string, string> CreateProducer()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = options.Value.BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true,
            MessageSendMaxRetries = 5,
            LingerMs = 5
        };

        return new ProducerBuilder<string, string>(config).Build();
    }
}
