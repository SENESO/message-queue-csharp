namespace Mq.Core;

/// <summary>
/// One immutable-once-rolled log segment: an append-only .log file plus a
/// sparse .index mapping offsets to file positions.
///
/// Index entry layout: [int64 offset][int64 position] (16 bytes). An entry
/// is written every <see cref="IndexIntervalBytes"/> appended bytes, so a
/// lookup seeks near the target and then scans forward record by record.
/// </summary>
public sealed class Segment : IDisposable
{
    public const int IndexEntrySize = sizeof(long) + sizeof(long);

    private readonly FileStream _log;
    private readonly FileStream _index;
    private readonly List<(long Offset, long Position)> _indexEntries = new();
    private long _bytesSinceIndex;

    public string LogPath { get; }
    public string IndexPath { get; }
    public long BaseOffset { get; }
    public long NextOffset { get; private set; }
    public long SizeBytes { get; private set; }
    public long MaxTimestampMs { get; private set; } = -1;
    public int IndexIntervalBytes { get; }

    public bool IsEmpty => NextOffset == BaseOffset;

    private Segment(string logPath, string indexPath, long baseOffset, int indexIntervalBytes)
    {
        LogPath = logPath;
        IndexPath = indexPath;
        BaseOffset = baseOffset;
        NextOffset = baseOffset;
        IndexIntervalBytes = indexIntervalBytes;
        _log = new FileStream(logPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
        _index = new FileStream(indexPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
        SizeBytes = _log.Length;
    }

    public static Segment Open(string directory, long baseOffset, int indexIntervalBytes)
    {
        Directory.CreateDirectory(directory);
        string logPath = Path.Combine(directory, FileName(baseOffset, ".log"));
        string indexPath = Path.Combine(directory, FileName(baseOffset, ".index"));
        var segment = new Segment(logPath, indexPath, baseOffset, indexIntervalBytes);
        segment.Recover();
        return segment;
    }

    public static string FileName(long baseOffset, string extension) => $"{baseOffset:D20}{extension}";

    /// <summary>
    /// Rebuilds in-memory state from disk: replays the log, rebuilds the
    /// sparse index, and truncates any torn record left by a crash mid-write.
    /// </summary>
    private void Recover()
    {
        _indexEntries.Clear();
        long position = 0;
        long length = _log.Length;
        long bytesSinceIndex = 0;
        var lengthBuffer = new byte[RecordCodec.LengthPrefixSize];

        while (position < length)
        {
            _log.Seek(position, SeekOrigin.Begin);
            if (!TryReadExact(_log, lengthBuffer, length - position))
            {
                Truncate(position);
                break;
            }
            int bodyLength = BitConverter.ToInt32(lengthBuffer);
            int totalLength = RecordCodec.LengthPrefixSize + bodyLength;
            if (bodyLength <= 0 || position + totalLength > length)
            {
                Truncate(position);
                break;
            }

            var buffer = new byte[totalLength];
            _log.Seek(position, SeekOrigin.Begin);
            if (!TryReadExact(_log, buffer, length - position))
            {
                Truncate(position);
                break;
            }

            Record record;
            try
            {
                record = RecordCodec.Decode(buffer, out _);
            }
            catch (CorruptedRecordException)
            {
                Truncate(position);
                break;
            }

            if (bytesSinceIndex >= IndexIntervalBytes || _indexEntries.Count == 0)
            {
                _indexEntries.Add((record.Offset, position));
                bytesSinceIndex = 0;
            }

            NextOffset = record.Offset + 1;
            MaxTimestampMs = Math.Max(MaxTimestampMs, record.TimestampMs);
            position += totalLength;
            bytesSinceIndex += totalLength;
        }

        SizeBytes = position;
        _log.SetLength(position);
        _log.Seek(position, SeekOrigin.Begin);

        // Rewrite the index file so it always matches memory after recovery.
        _index.SetLength(0);
        foreach (var (offset, pos) in _indexEntries)
            WriteIndexEntry(offset, pos);
        _index.Flush();
        _bytesSinceIndex = 0;
    }

    private void Truncate(long position)
    {
        _log.SetLength(position);
    }

    private static bool TryReadExact(Stream stream, byte[] buffer, long remaining)
    {
        if (remaining < buffer.Length)
            return false;
        int read = 0;
        while (read < buffer.Length)
        {
            int n = stream.Read(buffer, read, buffer.Length - read);
            if (n == 0)
                return false;
            read += n;
        }
        return true;
    }

    /// <summary>Appends an already-encoded record; the record's offset must equal <see cref="NextOffset"/>.</summary>
    public void Append(Record record)
    {
        if (record.Offset != NextOffset)
            throw new InvalidOperationException($"Offset gap: expected {NextOffset}, got {record.Offset}.");

        byte[] encoded = RecordCodec.Encode(record);
        if (_bytesSinceIndex >= IndexIntervalBytes)
        {
            WriteIndexEntry(record.Offset, SizeBytes);
            _indexEntries.Add((record.Offset, SizeBytes));
            _bytesSinceIndex = 0;
        }

        _log.Seek(SizeBytes, SeekOrigin.Begin);
        _log.Write(encoded);
        SizeBytes += encoded.Length;
        _bytesSinceIndex += encoded.Length;
        NextOffset = record.Offset + 1;
        MaxTimestampMs = Math.Max(MaxTimestampMs, record.TimestampMs);
    }

    private void WriteIndexEntry(long offset, long position)
    {
        Span<byte> entry = stackalloc byte[IndexEntrySize];
        BitConverter.TryWriteBytes(entry, offset);
        BitConverter.TryWriteBytes(entry.Slice(sizeof(long)), position);
        _index.Seek(0, SeekOrigin.End);
        _index.Write(entry);
        _index.Flush();
    }

    public void Flush() => _log.Flush(true);

    /// <summary>
    /// Reads up to <paramref name="maxRecords"/> records starting at <paramref name="offset"/>,
    /// stopping early when <paramref name="maxBytes"/> would be exceeded.
    /// </summary>
    public IReadOnlyList<Record> Read(long offset, int maxRecords, long maxBytes)
    {
        var results = new List<Record>();
        if (offset >= NextOffset || IsEmpty)
            return results;

        long position = FindStartPosition(offset);
        long length = SizeBytes;
        long bytesRead = 0;

        while (position < length && results.Count < maxRecords && bytesRead < maxBytes)
        {
            _log.Seek(position, SeekOrigin.Begin);
            var lengthBuffer = new byte[RecordCodec.LengthPrefixSize];
            if (!TryReadExact(_log, lengthBuffer, length - position))
                break;
            int bodyLength = BitConverter.ToInt32(lengthBuffer);
            int totalLength = RecordCodec.LengthPrefixSize + bodyLength;
            if (bodyLength <= 0 || position + totalLength > length)
                break;

            var buffer = new byte[totalLength];
            _log.Seek(position, SeekOrigin.Begin);
            if (!TryReadExact(_log, buffer, length - position))
                break;

            Record record = RecordCodec.Decode(buffer, out _);
            if (record.Offset >= offset)
            {
                results.Add(record);
                bytesRead += totalLength;
            }
            position += totalLength;
        }

        return results;
    }

    private long FindStartPosition(long offset)
    {
        // Floor entry: last index entry whose offset <= target.
        int lo = 0, hi = _indexEntries.Count - 1, best = -1;
        while (lo <= hi)
        {
            int mid = (lo + hi) / 2;
            if (_indexEntries[mid].Offset <= offset)
            {
                best = mid;
                lo = mid + 1;
            }
            else
            {
                hi = mid - 1;
            }
        }
        return best >= 0 ? _indexEntries[best].Position : 0;
    }

    public void Delete()
    {
        _log.Dispose();
        _index.Dispose();
        File.Delete(LogPath);
        File.Delete(IndexPath);
    }

    public void Dispose()
    {
        _log.Dispose();
        _index.Dispose();
    }
}
