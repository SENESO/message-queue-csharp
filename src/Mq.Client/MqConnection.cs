using System.Net.Sockets;
using Mq.Protocol;

namespace Mq.Client;

/// <summary>
/// One synchronous request/response TCP connection to a broker.
/// Requests are serialized with a lock; correlation ids match responses.
/// </summary>
public sealed class MqConnection : IDisposable
{
    private readonly TcpClient _client = new();
    private readonly SemaphoreSlim _send = new(1, 1);
    private NetworkStream? _stream;
    private int _nextCorrelationId;

    public string Host { get; }
    public int Port { get; }

    public MqConnection(string host = "127.0.0.1", int port = 9092)
    {
        Host = host;
        Port = port;
    }

    public async Task ConnectAsync(CancellationToken ct = default)
    {
        await _client.ConnectAsync(Host, Port, ct);
        _stream = _client.GetStream();
    }

    public async Task<byte[]> RequestAsync(ApiKey api, byte[] payload, CancellationToken ct = default)
    {
        if (_stream is null)
            throw new InvalidOperationException("Call ConnectAsync first.");

        await _send.WaitAsync(ct);
        try
        {
            int correlationId = Interlocked.Increment(ref _nextCorrelationId);
            await Frame.WriteAsync(_stream, api, correlationId, payload, ct);
            var frame = await Frame.ReadAsync(_stream, ct);
            if (frame is null)
                throw new IOException("Broker closed the connection.");
            if (frame.Value.CorrelationId != correlationId)
                throw new IOException("Correlation id mismatch: connection is out of sync.");
            return frame.Value.Payload;
        }
        finally
        {
            _send.Release();
        }
    }

    public void Dispose()
    {
        _client.Dispose();
        _send.Dispose();
    }
}
