using System.Text;

namespace Mq.Protocol;

/// <summary>Append-only binary writer for request/response payloads.</summary>
public sealed class PayloadWriter
{
    private readonly MemoryStream _stream = new();

    public void WriteInt32(int value) => Write(BitConverter.GetBytes(value));
    public void WriteInt64(long value) => Write(BitConverter.GetBytes(value));

    public void WriteString(string value)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(value);
        WriteInt32(bytes.Length);
        Write(bytes);
    }

    /// <summary>Length-prefixed bytes; -1 length encodes null.</summary>
    public void WriteBytes(byte[]? value)
    {
        if (value is null)
        {
            WriteInt32(-1);
            return;
        }
        WriteInt32(value.Length);
        Write(value);
    }

    private void Write(byte[] bytes) => _stream.Write(bytes, 0, bytes.Length);

    public byte[] ToArray() => _stream.ToArray();
}

/// <summary>Sequential binary reader for request/response payloads.</summary>
public sealed class PayloadReader
{
    private readonly byte[] _buffer;
    private int _position;

    public PayloadReader(byte[] buffer) => _buffer = buffer;

    public bool EndOfData => _position >= _buffer.Length;

    public int ReadInt32()
    {
        int value = BitConverter.ToInt32(_buffer, _position);
        _position += sizeof(int);
        return value;
    }

    public long ReadInt64()
    {
        long value = BitConverter.ToInt64(_buffer, _position);
        _position += sizeof(long);
        return value;
    }

    public string ReadString()
    {
        int length = ReadInt32();
        string value = Encoding.UTF8.GetString(_buffer, _position, length);
        _position += length;
        return value;
    }

    public byte[]? ReadBytes()
    {
        int length = ReadInt32();
        if (length < 0)
            return null;
        var value = new byte[length];
        Array.Copy(_buffer, _position, value, 0, length);
        _position += length;
        return value;
    }
}
