namespace Mq.Core;

/// <summary>
/// One partition of a topic: an ordered, append-only log. The unit of
/// parallelism and ordering - records are ordered within a partition only.
/// </summary>
public sealed class Partition : IDisposable
{
    private readonly SegmentedLog _log;

    public string Topic { get; }
    public int Id { get; }
    public long NextOffset => _log.NextOffset;
    public long BaseOffset => _log.BaseOffset;

    public Partition(string topic, int id, string directory, LogOptions? options = null)
    {
        Topic = topic;
        Id = id;
        _log = new SegmentedLog(directory, options);
    }

    public long Append(byte[]? key, byte[] value, long timestampMs) => _log.Append(key, value, timestampMs);

    public IReadOnlyList<Record> Read(long offset, int maxRecords = 1000, long maxBytes = 4 * 1024 * 1024) =>
        _log.Read(offset, maxRecords, maxBytes);

    public void Flush() => _log.Flush();

    public int ApplyRetention(long nowMs) => _log.ApplyRetention(nowMs);

    public void Dispose() => _log.Dispose();
}
