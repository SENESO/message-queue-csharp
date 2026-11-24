using System.Text;
using Mq.Broker;
using Mq.Client;

namespace Mq.Tests;

/// <summary>
/// Full stack: real broker over loopback TCP, real producer and consumer.
/// </summary>
public sealed class EndToEndTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "mq-tests", Guid.NewGuid().ToString("N"));

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    [Fact]
    public async Task Produce_ThenConsume_RoundTripsMessages()
    {
        using var broker = new BrokerServer(_dir, port: 0, new Mq.Core.LogOptions { MaxSegmentBytes = 512, IndexIntervalBytes = 32 });
        broker.Run();

        using var producerConnection = new MqConnection("127.0.0.1", broker.Port);
        await producerConnection.ConnectAsync();
        var producer = new MqProducer(producerConnection);
        await producer.CreateTopicAsync("orders", partitions: 2);

        for (int i = 0; i < 20; i++)
            await producer.SendAsync("orders", Bytes($"customer-{i % 3}"), Bytes($"order-{i}"));

        using var consumerConnection = new MqConnection("127.0.0.1", broker.Port);
        await consumerConnection.ConnectAsync();
        var consumer = new MqConsumer(consumerConnection, "billing");
        await consumer.SubscribeAsync("orders");

        var received = new List<Message>();
        for (int attempt = 0; attempt < 50 && received.Count < 20; attempt++)
        {
            IReadOnlyList<Message> batch = await consumer.PollAsync(maxRecords: 10);
            received.AddRange(batch);
            if (batch.Count == 0)
                await Task.Delay(10);
        }

        Assert.Equal(20, received.Count);
        Assert.Contains(received, m => Encoding.UTF8.GetString(m.Value) == "order-0");
        Assert.Contains(received, m => Encoding.UTF8.GetString(m.Value) == "order-19");
        // Keys route deterministically: all messages for one customer landed in one partition.
        Assert.Single(received.Where(m => Encoding.UTF8.GetString(m.Key!) == "customer-1").Select(m => m.Partition).Distinct());
    }

    [Fact]
    public async Task CommittedOffsets_ResumeAcrossConsumerRestarts()
    {
        using var broker = new BrokerServer(_dir, port: 0);
        broker.Run();

        using (var connection = new MqConnection("127.0.0.1", broker.Port))
        {
            await connection.ConnectAsync();
            var producer = new MqProducer(connection);
            await producer.CreateTopicAsync("jobs", partitions: 1);
            for (int i = 0; i < 5; i++)
                await producer.SendAsync("jobs", null, Bytes($"job-{i}"));
        }

        // First consumer reads 2 of 5 and commits.
        using (var connection = new MqConnection("127.0.0.1", broker.Port))
        {
            await connection.ConnectAsync();
            var consumer = new MqConsumer(connection, "workers");
            await consumer.SubscribeAsync("jobs");
            IReadOnlyList<Message> batch = await consumer.PollAsync(maxRecords: 2);
            Assert.Equal(2, batch.Count);
            await consumer.CommitAsync(batch[^1]);
        }

        // A fresh consumer in the same group resumes after the committed offset.
        using (var connection = new MqConnection("127.0.0.1", broker.Port))
        {
            await connection.ConnectAsync();
            var consumer = new MqConsumer(connection, "workers");
            await consumer.SubscribeAsync("jobs");
            IReadOnlyList<Message> batch = await consumer.PollAsync(maxRecords: 10);
            Assert.Equal(3, batch.Count);
            Assert.Equal("job-2", Encoding.UTF8.GetString(batch[0].Value));
        }
    }

    [Fact]
    public async Task UnknownTopic_ProduceThrows()
    {
        using var broker = new BrokerServer(_dir, port: 0);
        broker.Run();
        using var connection = new MqConnection("127.0.0.1", broker.Port);
        await connection.ConnectAsync();
        var producer = new MqProducer(connection);
        await Assert.ThrowsAsync<InvalidOperationException>(() => producer.SendAsync("ghost", null, Bytes("x")));
    }

    [Fact]
    public async Task BrokerRestart_KeepsDataAndOffsets()
    {
        int port;
        using (var broker = new BrokerServer(_dir, port: 0))
        {
            broker.Run();
            port = broker.Port;
            using var connection = new MqConnection("127.0.0.1", port);
            await connection.ConnectAsync();
            var producer = new MqProducer(connection);
            await producer.CreateTopicAsync("durable", partitions: 1);
            await producer.SendAsync("durable", null, Bytes("persisted"));
            var consumer = new MqConsumer(connection, "g");
            await consumer.SubscribeAsync("durable");
            IReadOnlyList<Message> batch = await consumer.PollAsync();
            await consumer.CommitAsync(batch[^1]);
        }

        using (var broker = new BrokerServer(_dir, port: 0))
        {
            broker.Run();
            using var connection = new MqConnection("127.0.0.1", broker.Port);
            await connection.ConnectAsync();
            var consumer = new MqConsumer(connection, "g");
            await consumer.SubscribeAsync("durable");
            IReadOnlyList<Message> batch = await consumer.PollAsync();
            Assert.Empty(batch); // committed offset survived the restart
        }
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }
}
