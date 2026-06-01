using KafkaConsumerLab.Worker.Configuration;
using Microsoft.Extensions.Options;

namespace KafkaConsumerLab.Worker.Reliability;

public sealed class BackoffCalculator(IOptions<RetryOptions> options)
{
    public int CalculateDelayMs(int attempt)
    {
        if (attempt < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(attempt), "Attempt must be at least 1.");
        }

        var retry = options.Value;
        var multiplier = Math.Pow(2, attempt - 1);
        var value = (int)Math.Min(retry.MaxDelayMs, retry.BaseDelayMs * multiplier);
        return value;
    }
}
