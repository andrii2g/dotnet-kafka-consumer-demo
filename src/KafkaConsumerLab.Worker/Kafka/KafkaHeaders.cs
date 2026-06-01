using System.Text;
using Confluent.Kafka;

namespace KafkaConsumerLab.Worker.Kafka;

public static class KafkaHeaders
{
    public const string OriginalTopic = "x-original-topic";
    public const string OriginalPartition = "x-original-partition";
    public const string OriginalOffset = "x-original-offset";
    public const string OriginalKey = "x-original-key";
    public const string ErrorType = "x-error-type";
    public const string ErrorMessage = "x-error-message";
    public const string Attempt = "x-attempt";
    public const string FirstFailedAtUtc = "x-first-failed-at-utc";
    public const string LastFailedAtUtc = "x-last-failed-at-utc";
    public const string NextDelayMs = "x-next-delay-ms";

    public static string? GetString(Headers headers, string key)
    {
        if (!headers.TryGetLastBytes(key, out var bytes) || bytes is null)
        {
            return null;
        }

        try
        {
            return Encoding.UTF8.GetString(bytes);
        }
        catch (DecoderFallbackException)
        {
            return null;
        }
    }

    public static void SetString(Headers headers, string key, string? value)
    {
        if (value is null)
        {
            return;
        }

        headers.Remove(key);
        headers.Add(key, Encoding.UTF8.GetBytes(value));
    }

    public static Headers Clone(Headers source)
    {
        var clone = new Headers();

        foreach (var header in source)
        {
            clone.Add(header.Key, header.GetValueBytes());
        }

        return clone;
    }
}
