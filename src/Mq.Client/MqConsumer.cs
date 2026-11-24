using Mq.Protocol;

namespace Mq.Client;

public sealed record Message(long Offset, long TimestampMs, byte[]? Key, byte[] Value, string Topic, int Partition);

/// <summary>
/// Consumer-group reader: resumes from the group's committed offset,
/// polls partitions round-robin, commits after processing.
/// </summary>
public sealed class MqConsumer
{
    private readonly MqConnection _connection;
    private readonly string _group;
    private readonly List<(string Topic, int Partition)> _assignment = new();
    private readonly Dictionary<(string, int), long> _positions = new();
    private int _nextPartition;

    public MqConsumer(MqConnection connection, string group)
    {
        _connection = connection;
        _group = group;
    }

    /// <summary>Assigns every partition of the topic to this consumer and resumes from committed offsets.</summary>
    public async Task SubscribeAsync(string topic, CancellationToken ct = default)
    {
        var producer = new MqProducer(_connection);
        var topics = await producer.ListTopicsAsync(ct);
        var found = topics.FirstOrDefault(t => t.Name == topic);
        if (found == default)
            throw new InvalidOperationException($"Unknown topic '{topic}'.");

        for (int partition = 0; partition < found.Partitions; partition++)
        {
            var writer = new PayloadWriter();
            writer.WriteString(_group);
            writer.WriteString(topic);
            writer.WriteInt32(partition);
            byte[] response = await _connection.RequestAsync(ApiKey.FetchOffset, writer.ToArray(), ct);
            var reader = new PayloadReader(response);
            MqProducer.ThrowOnError(reader.ReadInt32(), topic);
            long committed = reader.ReadInt64();

            _assignment.Add((topic, partition));
            _positions[(topic, partition)] = committed >= 0 ? committed + 1 : 0;
        }
    }

    /// <summary>Polls the next assigned partition; returns messages or an empty list when nothing is available.</summary>
    public async Task<IReadOnlyList<Message>> PollAsync(int maxRecords = 100, CancellationToken ct = default)
    {
        if (_assignment.Count == 0)
            throw new InvalidOperationException("Call SubscribeAsync first.");

        for (int attempt = 0; attempt < _assignment.Count; attempt++)
        {
            var (topic, partition) = _assignment[_nextPartition];
            _nextPartition = (_nextPartition + 1) % _assignment.Count;
            long offset = _positions[(topic, partition)];

            var writer = new PayloadWriter();
            writer.WriteString(topic);
            writer.WriteInt32(partition);
            writer.WriteInt64(offset);
            writer.WriteInt32(maxRecords);

            byte[] response = await _connection.RequestAsync(ApiKey.Fetch, writer.ToArray(), ct);
            var reader = new PayloadReader(response);
            MqProducer.ThrowOnError(reader.ReadInt32(), topic);
            _ = reader.ReadInt64(); // high watermark
            int count = reader.ReadInt32();
            if (count == 0)
                continue;

            var messages = new List<Message>(count);
            for (int i = 0; i < count; i++)
            {
                long recordOffset = reader.ReadInt64();
                long timestampMs = reader.ReadInt64();
                byte[]? key = reader.ReadBytes();
                byte[] value = reader.ReadBytes()!;
                messages.Add(new Message(recordOffset, timestampMs, key, value, topic, partition));
            }
            _positions[(topic, partition)] = messages[^1].Offset + 1;
            return messages;
        }
        return Array.Empty<Message>();
    }

    /// <summary>Commits the offset after the last processed message for its partition.</summary>
    public async Task CommitAsync(Message lastProcessed, CancellationToken ct = default)
    {
        var writer = new PayloadWriter();
        writer.WriteString(_group);
        writer.WriteString(lastProcessed.Topic);
        writer.WriteInt32(lastProcessed.Partition);
        writer.WriteInt64(lastProcessed.Offset);
        byte[] response = await _connection.RequestAsync(ApiKey.CommitOffset, writer.ToArray(), ct);
        MqProducer.ThrowOnError(new PayloadReader(response).ReadInt32(), lastProcessed.Topic);
    }
}
