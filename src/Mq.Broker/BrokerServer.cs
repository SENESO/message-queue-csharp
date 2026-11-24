using System.Net;
using System.Net.Sockets;
using Mq.Core;
using Mq.Protocol;

namespace Mq.Broker;

/// <summary>
/// TCP broker: accepts client connections, reads frames, dispatches to the
/// topic manager and group offset store. One task per connection.
/// </summary>
public sealed class BrokerServer : IDisposable
{
    private readonly TcpListener _listener;
    private readonly TopicManager _topics;
    private readonly GroupOffsetStore _offsets;
    private readonly CancellationTokenSource _shutdown = new();
    private readonly List<Task> _sessions = new();

    public int Port { get; }

    public BrokerServer(string dataDirectory, int port, LogOptions? options = null)
    {
        _topics = new TopicManager(dataDirectory, options);
        _offsets = new GroupOffsetStore(dataDirectory);
        _listener = new TcpListener(IPAddress.Loopback, port);
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
    }

    public void Run()
    {
        Task.Run(AcceptLoop);
    }

    private async Task AcceptLoop()
    {
        try
        {
            while (!_shutdown.IsCancellationRequested)
            {
                TcpClient client = await _listener.AcceptTcpClientAsync(_shutdown.Token);
                lock (_sessions)
                    _sessions.Add(Task.Run(() => HandleSession(client)));
            }
        }
        catch (OperationCanceledException) { }
        catch (SocketException) { }
    }

    private async Task HandleSession(TcpClient client)
    {
        using (client)
        {
            NetworkStream stream = client.GetStream();
            try
            {
                while (!_shutdown.IsCancellationRequested)
                {
                    var frame = await Frame.ReadAsync(stream, _shutdown.Token);
                    if (frame is null)
                        return;
                    var (api, correlationId, payload) = frame.Value;
                    byte[] response = Dispatch(api, payload);
                    await Frame.WriteAsync(stream, api, correlationId, response, _shutdown.Token);
                }
            }
            catch (InvalidDataException) { }
            catch (IOException) { }
            catch (OperationCanceledException) { }
            catch (SocketException) { }
        }
    }

    private byte[] Dispatch(ApiKey api, byte[] payload)
    {
        try
        {
            return api switch
            {
                ApiKey.CreateTopic => HandleCreateTopic(payload),
                ApiKey.ListTopics => HandleListTopics(),
                ApiKey.Produce => HandleProduce(payload),
                ApiKey.Fetch => HandleFetch(payload),
                ApiKey.CommitOffset => HandleCommitOffset(payload),
                ApiKey.FetchOffset => HandleFetchOffset(payload),
                _ => Error(ErrorCode.BadRequest),
            };
        }
        catch (TopicNotFoundException)
        {
            return Error(ErrorCode.UnknownTopic);
        }
        catch (CorruptedRecordException)
        {
            return Error(ErrorCode.Internal);
        }
        catch (Exception ex) when (ex is InvalidOperationException or ArgumentException)
        {
            return Error(ErrorCode.BadRequest);
        }
    }

    private byte[] HandleCreateTopic(byte[] payload)
    {
        var reader = new PayloadReader(payload);
        string topic = reader.ReadString();
        int partitions = reader.ReadInt32();
        _topics.CreateTopic(topic, partitions);

        var writer = new PayloadWriter();
        writer.WriteInt32(ErrorCode.None);
        return writer.ToArray();
    }

    private byte[] HandleListTopics()
    {
        var writer = new PayloadWriter();
        writer.WriteInt32(ErrorCode.None);
        var topics = _topics.ListTopics();
        writer.WriteInt32(topics.Count);
        foreach (string topic in topics)
        {
            writer.WriteString(topic);
            writer.WriteInt32(_topics.PartitionCount(topic));
        }
        return writer.ToArray();
    }

    private byte[] HandleProduce(byte[] payload)
    {
        var reader = new PayloadReader(payload);
        string topic = reader.ReadString();
        int partition = reader.ReadInt32(); // -1 = route by key
        byte[]? key = reader.ReadBytes();
        byte[] value = reader.ReadBytes() ?? Array.Empty<byte>();
        long timestampMs = reader.ReadInt64();

        if (partition < 0)
            partition = _topics.RoutePartition(topic, key);

        long offset = _topics.GetPartition(topic, partition).Append(key, value, timestampMs);

        var writer = new PayloadWriter();
        writer.WriteInt32(ErrorCode.None);
        writer.WriteInt32(partition);
        writer.WriteInt64(offset);
        return writer.ToArray();
    }

    private byte[] HandleFetch(byte[] payload)
    {
        var reader = new PayloadReader(payload);
        string topic = reader.ReadString();
        int partition = reader.ReadInt32();
        long offset = reader.ReadInt64();
        int maxRecords = reader.ReadInt32();

        Partition p = _topics.GetPartition(topic, partition);
        if (offset < p.BaseOffset)
            return Error(ErrorCode.OffsetOutOfRange);

        IReadOnlyList<Core.Record> records = p.Read(offset, maxRecords);

        var writer = new PayloadWriter();
        writer.WriteInt32(ErrorCode.None);
        writer.WriteInt64(p.NextOffset); // high watermark
        writer.WriteInt32(records.Count);
        foreach (Core.Record record in records)
        {
            writer.WriteInt64(record.Offset);
            writer.WriteInt64(record.TimestampMs);
            writer.WriteBytes(record.Key);
            writer.WriteBytes(record.Value);
        }
        return writer.ToArray();
    }

    private byte[] HandleCommitOffset(byte[] payload)
    {
        var reader = new PayloadReader(payload);
        string group = reader.ReadString();
        string topic = reader.ReadString();
        int partition = reader.ReadInt32();
        long offset = reader.ReadInt64();

        _offsets.Commit(group, topic, partition, offset);

        var writer = new PayloadWriter();
        writer.WriteInt32(ErrorCode.None);
        return writer.ToArray();
    }

    private byte[] HandleFetchOffset(byte[] payload)
    {
        var reader = new PayloadReader(payload);
        string group = reader.ReadString();
        string topic = reader.ReadString();
        int partition = reader.ReadInt32();

        long? offset = _offsets.Get(group, topic, partition);

        var writer = new PayloadWriter();
        writer.WriteInt32(ErrorCode.None);
        writer.WriteInt64(offset ?? -1);
        return writer.ToArray();
    }

    private static byte[] Error(int code)
    {
        var writer = new PayloadWriter();
        writer.WriteInt32(code);
        return writer.ToArray();
    }

    public void Dispose()
    {
        _shutdown.Cancel();
        _listener.Stop();
        Task[] sessions;
        lock (_sessions)
            sessions = _sessions.ToArray();
        try { Task.WaitAll(sessions, TimeSpan.FromSeconds(2)); } catch { }
        _topics.Dispose();
        _shutdown.Dispose();
    }
}
