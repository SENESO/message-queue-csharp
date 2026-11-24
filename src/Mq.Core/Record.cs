namespace Mq.Core;

/// <summary>
/// One message stored in a partition log.
/// </summary>
/// <param name="Offset">Partition-relative, monotonically assigned by the log.</param>
/// <param name="TimestampMs">Producer time, milliseconds since Unix epoch.</param>
/// <param name="Key">Optional routing key; null when absent.</param>
/// <param name="Value">Message payload.</param>
public sealed record Record(long Offset, long TimestampMs, byte[]? Key, byte[] Value)
{
    public int KeyLength => Key?.Length ?? 0;
}

/// <summary>
/// Binary record codec.
///
/// On-disk layout (all little-endian):
///   [int32 length]        bytes after this field up to and including the CRC
///   [int64 offset]
///   [int64 timestampMs]
///   [int32 keyLength]     -1 when the key is null
///   [key bytes]
///   [int32 valueLength]
///   [value bytes]
///   [uint32 crc32]        over every byte after the length field, before the CRC
/// </summary>
public static class RecordCodec
{
    public const int LengthPrefixSize = sizeof(int);
    public const int CrcSize = sizeof(uint);
    public const int HeaderSize = sizeof(long) + sizeof(long) + sizeof(int); // offset + ts + keyLength

    public static int EncodedSize(Record record) =>
        LengthPrefixSize + HeaderSize + record.KeyLength + sizeof(int) + record.Value.Length + CrcSize;

    /// <summary>Serializes the record into <paramref name="destination"/>, which must be exactly <see cref="EncodedSize(Record)"/> bytes.</summary>
    public static void Encode(Record record, Span<byte> destination)
    {
        int bodyLength = EncodedSize(record) - LengthPrefixSize;
        var writer = new SpanWriter(destination);
        writer.WriteInt32(bodyLength);
        int crcStart = writer.Position;
        writer.WriteInt64(record.Offset);
        writer.WriteInt64(record.TimestampMs);
        writer.WriteInt32(record.Key is null ? -1 : record.Key.Length);
        if (record.Key is not null)
            writer.WriteBytes(record.Key);
        writer.WriteInt32(record.Value.Length);
        writer.WriteBytes(record.Value);
        uint crc = Crc32.Compute(destination.Slice(crcStart, writer.Position - crcStart));
        writer.WriteUInt32(crc);
    }

    public static byte[] Encode(Record record)
    {
        var buffer = new byte[EncodedSize(record)];
        Encode(record, buffer);
        return buffer;
    }

    /// <summary>
    /// Decodes one record from the start of <paramref name="source"/> (which begins at a length prefix).
    /// Throws <see cref="CorruptedRecordException"/> when the CRC does not match.
    /// </summary>
    public static Record Decode(ReadOnlySpan<byte> source, out int bytesConsumed)
    {
        var reader = new SpanReader(source);
        int bodyLength = reader.ReadInt32();
        if (bodyLength < HeaderSize + sizeof(int) + CrcSize)
            throw new CorruptedRecordException($"Implausible record length {bodyLength}.");
        if (source.Length < LengthPrefixSize + bodyLength)
            throw new CorruptedRecordException("Truncated record: not enough bytes available.");

        var body = source.Slice(LengthPrefixSize, bodyLength - CrcSize);
        uint expectedCrc = reader.ReadUInt32At(LengthPrefixSize + bodyLength - CrcSize);
        uint actualCrc = Crc32.Compute(body);
        if (expectedCrc != actualCrc)
            throw new CorruptedRecordException("CRC mismatch: record is corrupted.");

        long offset = reader.ReadInt64();
        long timestampMs = reader.ReadInt64();
        int keyLength = reader.ReadInt32();
        byte[]? key = keyLength < 0 ? null : reader.ReadBytes(keyLength).ToArray();
        int valueLength = reader.ReadInt32();
        byte[] value = reader.ReadBytes(valueLength).ToArray();

        bytesConsumed = LengthPrefixSize + bodyLength;
        return new Record(offset, timestampMs, key, value);
    }
}

public sealed class CorruptedRecordException : Exception
{
    public CorruptedRecordException(string message) : base(message) { }
}

internal ref struct SpanWriter
{
    private readonly Span<byte> _span;
    public int Position { get; private set; }

    public SpanWriter(Span<byte> span) => _span = span;

    public void WriteInt32(int value)
    {
        BitConverter.TryWriteBytes(_span.Slice(Position), value);
        Position += sizeof(int);
    }

    public void WriteUInt32(uint value)
    {
        BitConverter.TryWriteBytes(_span.Slice(Position), value);
        Position += sizeof(uint);
    }

    public void WriteInt64(long value)
    {
        BitConverter.TryWriteBytes(_span.Slice(Position), value);
        Position += sizeof(long);
    }

    public void WriteBytes(ReadOnlySpan<byte> value)
    {
        value.CopyTo(_span.Slice(Position));
        Position += value.Length;
    }
}

internal ref struct SpanReader
{
    private readonly ReadOnlySpan<byte> _span;
    public int Position { get; private set; }

    public SpanReader(ReadOnlySpan<byte> span) => _span = span;

    public int ReadInt32()
    {
        int value = BitConverter.ToInt32(_span.Slice(Position));
        Position += sizeof(int);
        return value;
    }

    public uint ReadUInt32At(int position) => BitConverter.ToUInt32(_span.Slice(position));

    public long ReadInt64()
    {
        long value = BitConverter.ToInt64(_span.Slice(Position));
        Position += sizeof(long);
        return value;
    }

    public ReadOnlySpan<byte> ReadBytes(int count)
    {
        var slice = _span.Slice(Position, count);
        Position += count;
        return slice;
    }
}
