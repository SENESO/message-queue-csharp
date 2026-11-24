namespace Mq.Core;

public sealed class LogOptions
{
    /// <summary>Roll the active segment once it exceeds this many bytes.</summary>
    public long MaxSegmentBytes { get; init; } = 64 * 1024 * 1024;

    /// <summary>Sparse index granularity: one entry per this many appended bytes.</summary>
    public int IndexIntervalBytes { get; init; } = 4096;

    /// <summary>Retention: drop oldest whole segments while total size exceeds this. -1 keeps everything.</summary>
    public long RetentionBytes { get; init; } = -1;

    /// <summary>Retention: drop oldest whole segments whose newest record is older than this age. -1 keeps everything.</summary>
    public long RetentionMs { get; init; } = -1;
}

/// <summary>
/// Append-only, offset-ordered log for one partition, stored as a sequence
/// of rolled segments in a single directory.
/// </summary>
public sealed class SegmentedLog : IDisposable
{
    private readonly object _gate = new();
    private readonly List<Segment> _segments = new();
    private readonly LogOptions _options;

    public string Directory { get; }
    public long NextOffset { get; private set; }
    public long BaseOffset => _segments.Count > 0 ? _segments[0].BaseOffset : 0;

    public IReadOnlyList<Segment> Segments => _segments;

    public SegmentedLog(string directory, LogOptions? options = null)
    {
        Directory = directory;
        _options = options ?? new LogOptions();
        System.IO.Directory.CreateDirectory(directory);
        LoadSegments();
    }

    private void LoadSegments()
    {
        var bases = System.IO.Directory
            .EnumerateFiles(Directory, "*.log")
            .Select(Path.GetFileNameWithoutExtension)
            .Select(name => long.TryParse(name, out long b) ? b : (long?)null)
            .Where(b => b.HasValue)
            .Select(b => b!.Value)
            .OrderBy(b => b)
            .ToList();

        foreach (long baseOffset in bases)
            _segments.Add(Segment.Open(Directory, baseOffset, _options.IndexIntervalBytes));

        if (_segments.Count == 0)
            _segments.Add(Segment.Open(Directory, 0, _options.IndexIntervalBytes));

        NextOffset = _segments[^1].NextOffset;
    }

    private Segment ActiveSegment => _segments[^1];

    /// <summary>Assigns the next offset and appends the record. Returns the assigned offset.</summary>
    public long Append(byte[]? key, byte[] value, long timestampMs)
    {
        lock (_gate)
        {
            if (ActiveSegment.SizeBytes >= _options.MaxSegmentBytes && !ActiveSegment.IsEmpty)
            {
                ActiveSegment.Flush();
                _segments.Add(Segment.Open(Directory, NextOffset, _options.IndexIntervalBytes));
            }

            var record = new Record(NextOffset, timestampMs, key, value);
            ActiveSegment.Append(record);
            NextOffset = record.Offset + 1;
            return record.Offset;
        }
    }

    /// <summary>Reads records starting at <paramref name="offset"/> across segment boundaries.</summary>
    public IReadOnlyList<Record> Read(long offset, int maxRecords = 1000, long maxBytes = 4 * 1024 * 1024)
    {
        lock (_gate)
        {
            var results = new List<Record>();
            long bytesRead = 0;
            foreach (Segment segment in _segments)
            {
                if (segment.NextOffset <= offset)
                    continue;
                foreach (Record record in segment.Read(offset, maxRecords - results.Count, maxBytes - bytesRead))
                {
                    results.Add(record);
                    bytesRead += RecordCodec.EncodedSize(record);
                    if (results.Count >= maxRecords || bytesRead >= maxBytes)
                        return results;
                }
            }
            return results;
        }
    }

    public void Flush()
    {
        lock (_gate)
        {
            foreach (Segment segment in _segments)
                segment.Flush();
        }
    }

    /// <summary>
    /// Applies size and age retention by deleting oldest whole segments.
    /// The active segment is never deleted. Returns the number deleted.
    /// </summary>
    public int ApplyRetention(long nowMs)
    {
        lock (_gate)
        {
            int deleted = 0;
            bool overSize = _options.RetentionBytes >= 0 && _segments.Sum(s => s.SizeBytes) > _options.RetentionBytes;
            bool overAge = _options.RetentionMs >= 0 &&
                           _segments.Count > 1 &&
                           _segments[0].MaxTimestampMs >= 0 &&
                           nowMs - _segments[0].MaxTimestampMs > _options.RetentionMs;

            while (_segments.Count > 1 && (overSize || overAge))
            {
                Segment oldest = _segments[0];
                oldest.Delete();
                _segments.RemoveAt(0);
                deleted++;
                overSize = _options.RetentionBytes >= 0 && _segments.Sum(s => s.SizeBytes) > _options.RetentionBytes;
                overAge = _options.RetentionMs >= 0 &&
                          _segments.Count > 1 &&
                          _segments[0].MaxTimestampMs >= 0 &&
                          nowMs - _segments[0].MaxTimestampMs > _options.RetentionMs;
            }
            return deleted;
        }
    }

    public void Dispose()
    {
        foreach (Segment segment in _segments)
            segment.Dispose();
    }
}
