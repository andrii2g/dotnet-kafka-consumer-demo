using System.Collections.Concurrent;
using System.Text;

namespace KafkaConsumerLab.Worker.Diagnostics;

public sealed class ConsumerMetrics
{
    private readonly ConcurrentDictionary<string, long> _values = new(StringComparer.Ordinal);

    public void Increment(string metricName)
    {
        _values.AddOrUpdate(metricName, 1, static (_, current) => current + 1);
    }

    public string Snapshot()
    {
        var builder = new StringBuilder();

        foreach (var pair in _values.OrderBy(static item => item.Key, StringComparer.Ordinal))
        {
            if (builder.Length > 0)
            {
                builder.Append(", ");
            }

            builder.Append(pair.Key).Append('=').Append(pair.Value);
        }

        return builder.ToString();
    }
}
