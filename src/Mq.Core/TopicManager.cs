using System.Collections.Concurrent;

namespace Mq.Core;

/// <summary>
/// Owns every topic and partition on a broker, laid out on disk as
/// data/&lt;topic&gt;/&lt;partitionId&gt;/ (segments inside).
/// </summary>
public sealed class TopicManager : IDisposable
{
    private readonly ConcurrentDictionary<string, Partition[]> _topics = new(StringComparer.Ordinal);
    private readonly string _dataDirectory;
    private readonly LogOptions _options;

    public TopicManager(string dataDirectory, LogOptions? options = null)
    {
        _dataDirectory = dataDirectory;
        _options = options ?? new LogOptions();
        System.IO.Directory.CreateDirectory(_dataDirectory);
        LoadExisting();
    }

    private void LoadExisting()
    {
        foreach (string topicDir in System.IO.Directory.EnumerateDirectories(_dataDirectory))
        {
            string topic = Path.GetFileName(topicDir);
            var partitionIds = System.IO.Directory
                .EnumerateDirectories(topicDir)
                .Select(Path.GetFileName)
                .Select(name => int.TryParse(name, out int id) ? id : (int?)null)
                .Where(id => id.HasValue)
                .Select(id => id!.Value)
                .OrderBy(id => id)
                .ToList();
            if (partitionIds.Count > 0)
                OpenTopic(topic, partitionIds.Max() + 1);
        }
    }

    public IReadOnlyList<string> ListTopics() => _topics.Keys.OrderBy(t => t, StringComparer.Ordinal).ToList();

    public int PartitionCount(string topic) =>
        _topics.TryGetValue(topic, out Partition[]? partitions) ? partitions.Length : 0;

    /// <summary>Creates the topic when absent. Throws when it exists with a different partition count.</summary>
    public void CreateTopic(string topic, int partitions)
    {
        ValidateTopicName(topic);
        if (partitions < 1)
            throw new ArgumentOutOfRangeException(nameof(partitions), "A topic needs at least one partition.");

        if (_topics.TryGetValue(topic, out Partition[]? existing))
        {
            if (existing.Length != partitions)
                throw new InvalidOperationException(
                    $"Topic '{topic}' already exists with {existing.Length} partitions.");
            return;
        }
        OpenTopic(topic, partitions);
    }

    private void OpenTopic(string topic, int partitions)
    {
        var opened = new Partition[partitions];
        for (int i = 0; i < partitions; i++)
            opened[i] = new Partition(topic, i, Path.Combine(_dataDirectory, topic, i.ToString()), _options);
        _topics[topic] = opened;
    }

    public Partition GetPartition(string topic, int partitionId)
    {
        if (!_topics.TryGetValue(topic, out Partition[]? partitions))
            throw new TopicNotFoundException(topic);
        if (partitionId < 0 || partitionId >= partitions.Length)
            throw new InvalidOperationException($"Topic '{topic}' has no partition {partitionId}.");
        return partitions[partitionId];
    }

    /// <summary>Routes a key to a stable partition (null keys round-robin via offset parity fallback).</summary>
    public int RoutePartition(string topic, byte[]? key)
    {
        int count = PartitionCount(topic);
        if (count == 0)
            throw new TopicNotFoundException(topic);
        if (key is null || key.Length == 0)
            return 0;
        // FNV-1a 32-bit for stable, dependency-free key hashing.
        uint hash = 2166136261u;
        foreach (byte b in key)
            hash = (hash ^ b) * 16777619u;
        return (int)(hash % (uint)count);
    }

    public void FlushAll()
    {
        foreach (Partition[] partitions in _topics.Values)
            foreach (Partition partition in partitions)
                partition.Flush();
    }

    private static void ValidateTopicName(string topic)
    {
        if (string.IsNullOrWhiteSpace(topic) || topic.Length > 200 ||
            topic.Any(c => !(char.IsLetterOrDigit(c) || c is '-' or '_' or '.')))
            throw new ArgumentException($"Invalid topic name '{topic}'.", nameof(topic));
    }

    public void Dispose()
    {
        foreach (Partition[] partitions in _topics.Values)
            foreach (Partition partition in partitions)
                partition.Dispose();
    }
}

public sealed class TopicNotFoundException : Exception
{
    public TopicNotFoundException(string topic) : base($"Unknown topic '{topic}'.") { }
}
