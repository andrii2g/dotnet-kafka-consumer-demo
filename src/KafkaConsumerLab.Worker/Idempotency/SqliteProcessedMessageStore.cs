using KafkaConsumerLab.Worker.Configuration;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Options;

namespace KafkaConsumerLab.Worker.Idempotency;

public sealed class SqliteProcessedMessageStore(IOptions<IdempotencyOptions> options) : IProcessedMessageStore
{
    private const string CreateTableSql =
        """
        CREATE TABLE IF NOT EXISTS processed_messages (
            idempotency_key TEXT PRIMARY KEY,
            topic TEXT NOT NULL,
            partition INTEGER NOT NULL,
            offset INTEGER NOT NULL,
            processed_at_utc TEXT NOT NULL
        );
        """;

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        await using var connection = CreateConnection();
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        await using var command = connection.CreateCommand();
        command.CommandText = CreateTableSql;
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> ExistsAsync(string idempotencyKey, CancellationToken cancellationToken)
    {
        await using var connection = CreateConnection();
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM processed_messages WHERE idempotency_key = $key LIMIT 1;";
        command.Parameters.AddWithValue("$key", idempotencyKey);

        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is not null;
    }

    public async Task<bool> TryMarkProcessedAsync(string idempotencyKey, string topic, int partition, long offset, CancellationToken cancellationToken)
    {
        await using var connection = CreateConnection();
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            INSERT INTO processed_messages (idempotency_key, topic, partition, offset, processed_at_utc)
            VALUES ($key, $topic, $partition, $offset, $processedAtUtc);
            """;

        command.Parameters.AddWithValue("$key", idempotencyKey);
        command.Parameters.AddWithValue("$topic", topic);
        command.Parameters.AddWithValue("$partition", partition);
        command.Parameters.AddWithValue("$offset", offset);
        command.Parameters.AddWithValue("$processedAtUtc", DateTimeOffset.UtcNow.ToString("O"));

        try
        {
            return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1;
        }
        catch (SqliteException exception) when (exception.SqliteErrorCode == 19)
        {
            return false;
        }
    }

    private SqliteConnection CreateConnection() => new(options.Value.ConnectionString);
}
