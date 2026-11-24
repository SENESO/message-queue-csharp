using System.Buffers.Binary;

namespace Mq.Protocol;

public enum ApiKey : short
{
    CreateTopic = 1,
    ListTopics = 2,
    Produce = 3,
    Fetch = 4,
    CommitOffset = 5,
    FetchOffset = 6,
}

public static class ErrorCode
{
    public const int None = 0;
    public const int UnknownTopic = 1;
    public const int UnknownPartition = 2;
    public const int BadRequest = 3;
    public const int OffsetOutOfRange = 4;
    public const int Internal = 5;
}

/// <summary>
/// Wire framing: [int32 frameLength][int16 apiKey][int16 version][int32 correlationId][payload].
/// frameLength covers everything after itself. All integers little-endian.
/// </summary>
public static class Frame
{
    public const int HeaderSize = sizeof(short) + sizeof(short) + sizeof(int);
    public const int MaxFrameBytes = 16 * 1024 * 1024;
    public const short CurrentVersion = 1;

    public static async Task WriteAsync(Stream stream, ApiKey api, int correlationId, byte[] payload, CancellationToken ct)
    {
        int frameLength = HeaderSize + payload.Length;
        var buffer = new byte[sizeof(int) + frameLength];
        BinaryPrimitives.WriteInt32LittleEndian(buffer, frameLength);
        BinaryPrimitives.WriteInt16LittleEndian(buffer.AsSpan(sizeof(int)), (short)api);
        BinaryPrimitives.WriteInt16LittleEndian(buffer.AsSpan(sizeof(int) + sizeof(short)), CurrentVersion);
        BinaryPrimitives.WriteInt32LittleEndian(buffer.AsSpan(sizeof(int) + sizeof(short) * 2), correlationId);
        payload.CopyTo(buffer, sizeof(int) + HeaderSize);
        await stream.WriteAsync(buffer, ct);
        await stream.FlushAsync(ct);
    }

    public static async Task<(ApiKey Api, int CorrelationId, byte[] Payload)?> ReadAsync(Stream stream, CancellationToken ct)
    {
        var lengthBuffer = new byte[sizeof(int)];
        if (!await ReadExactAsync(stream, lengthBuffer, ct))
            return null;
        int frameLength = BinaryPrimitives.ReadInt32LittleEndian(lengthBuffer);
        if (frameLength < HeaderSize || frameLength > MaxFrameBytes)
            throw new InvalidDataException($"Bad frame length {frameLength}.");

        var frame = new byte[frameLength];
        if (!await ReadExactAsync(stream, frame, ct))
            throw new InvalidDataException("Connection closed mid-frame.");

        short api = BinaryPrimitives.ReadInt16LittleEndian(frame);
        int correlationId = BinaryPrimitives.ReadInt32LittleEndian(frame.AsSpan(sizeof(short) * 2));
        var payload = new byte[frameLength - HeaderSize];
        Array.Copy(frame, HeaderSize, payload, 0, payload.Length);
        return ((ApiKey)api, correlationId, payload);
    }

    private static async Task<bool> ReadExactAsync(Stream stream, byte[] buffer, CancellationToken ct)
    {
        int read = 0;
        while (read < buffer.Length)
        {
            int n = await stream.ReadAsync(buffer.AsMemory(read, buffer.Length - read), ct);
            if (n == 0)
                return false;
            read += n;
        }
        return true;
    }
}
