using System.Text.Json;

namespace Mq.Broker;

/// <summary>
/// Committed consumer offsets per (group, topic, partition), persisted as a
/// single JSON file so groups survive broker restarts.
/// </summary>
public sealed class GroupOffsetStore
{
    private readonly object _gate = new();
    private readonly Dictionary<string, long> _offsets = new();
    private readonly string _filePath;

    public GroupOffsetStore(string dataDirectory)
    {
        _filePath = Path.Combine(dataDirectory, "_consumer_offsets.json");
        Load();
    }

    private static string KeyOf(string group, string topic, int partition) => $"{group}{topic}{partition}";

    private void Load()
    {
        if (!File.Exists(_filePath))
            return;
        try
        {
            var entries = JsonSerializer.Deserialize<Dictionary<string, long>>(File.ReadAllText(_filePath));
            if (entries is not null)
                foreach (var (key, value) in entries)
                    _offsets[key] = value;
        }
        catch (JsonException)
        {
            // A corrupt offsets file must not kill the broker; consumers resume from their last good commit.
        }
    }

    public void Commit(string group, string topic, int partition, long offset)
    {
        lock (_gate)
        {
            _offsets[KeyOf(group, topic, partition)] = offset;
            Persist();
        }
    }

    public long? Get(string group, string topic, int partition)
    {
        lock (_gate)
        {
            return _offsets.TryGetValue(KeyOf(group, topic, partition), out long offset) ? offset : null;
        }
    }

    private void Persist()
    {
        string temp = _filePath + ".tmp";
        File.WriteAllText(temp, JsonSerializer.Serialize(_offsets));
        File.Move(temp, _filePath, overwrite: true);
    }
}
