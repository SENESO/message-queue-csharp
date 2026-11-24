using Mq.Protocol;

namespace Mq.Client;

public sealed record ProduceResult(int Partition, long Offset);

/// <summary>Produces records to a topic, routing by key when no partition is given.</summary>
public sealed class MqProducer
{
    private readonly MqConnection _connection;

    public MqProducer(MqConnection connection) => _connection = connection;

    public async Task CreateTopicAsync(string topic, int partitions = 1, CancellationToken ct = default)
    {
        var writer = new PayloadWriter();
        writer.WriteString(topic);
        writer.WriteInt32(partitions);
        byte[] response = await _connection.RequestAsync(ApiKey.CreateTopic, writer.ToArray(), ct);
        ThrowOnError(new PayloadReader(response).ReadInt32(), topic);
    }

    public async Task<IReadOnlyList<(string Name, int Partitions)>> ListTopicsAsync(CancellationToken ct = default)
    {
        byte[] response = await _connection.RequestAsync(ApiKey.ListTopics, Array.Empty<byte>(), ct);
        var reader = new PayloadReader(response);
        ThrowOnError(reader.ReadInt32(), null);
        int count = reader.ReadInt32();
        var topics = new List<(string, int)>(count);
        for (int i = 0; i < count; i++)
            topics.Add((reader.ReadString(), reader.ReadInt32()));
        return topics;
    }

    /// <summary>Sends one record. Partition -1 lets the broker route by key.</summary>
    public async Task<ProduceResult> SendAsync(string topic, byte[]? key, byte[] value, int partition = -1, CancellationToken ct = default)
    {
        var writer = new PayloadWriter();
        writer.WriteString(topic);
        writer.WriteInt32(partition);
        writer.WriteBytes(key);
        writer.WriteBytes(value);
        writer.WriteInt64(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

        byte[] response = await _connection.RequestAsync(ApiKey.Produce, writer.ToArray(), ct);
        var reader = new PayloadReader(response);
        ThrowOnError(reader.ReadInt32(), topic);
        return new ProduceResult(reader.ReadInt32(), reader.ReadInt64());
    }

    internal static void ThrowOnError(int code, string? topic)
    {
        if (code == ErrorCode.None)
            return;
        throw code switch
        {
            ErrorCode.UnknownTopic => new InvalidOperationException($"Unknown topic '{topic}'."),
            ErrorCode.UnknownPartition => new InvalidOperationException($"Unknown partition on topic '{topic}'."),
            ErrorCode.OffsetOutOfRange => new InvalidOperationException("Offset out of range."),
            ErrorCode.BadRequest => new InvalidOperationException("Broker rejected the request."),
            _ => new InvalidOperationException($"Broker error {code}."),
        };
    }
}
