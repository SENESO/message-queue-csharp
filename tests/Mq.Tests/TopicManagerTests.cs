using Mq.Core;
using Record = Mq.Core.Record;

namespace Mq.Tests;

public sealed class TopicManagerTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "mq-tests", Guid.NewGuid().ToString("N"));

    private static byte[] Bytes(string s) => System.Text.Encoding.UTF8.GetBytes(s);

    [Fact]
    public void CreateTopic_ThenAppendAndRead()
    {
        using var manager = new TopicManager(_dir);
        manager.CreateTopic("orders", partitions: 3);

        Partition p = manager.GetPartition("orders", 1);
        long offset = p.Append(Bytes("order-1"), Bytes("{}"), 100);
        Assert.Equal(0, offset);

        IReadOnlyList<Record> records = manager.GetPartition("orders", 1).Read(0);
        Assert.Single(records);
        Assert.Equal("orders", manager.ListTopics().Single());
        Assert.Equal(3, manager.PartitionCount("orders"));
    }

    [Fact]
    public void KeyRouting_IsStablePerKey()
    {
        using var manager = new TopicManager(_dir);
        manager.CreateTopic("events", partitions: 8);

        int first = manager.RoutePartition("events", Bytes("user-42"));
        for (int i = 0; i < 10; i++)
            Assert.Equal(first, manager.RoutePartition("events", Bytes("user-42")));
        Assert.InRange(first, 0, 7);
    }

    [Fact]
    public void UnknownTopic_ThrowsTopicNotFound()
    {
        using var manager = new TopicManager(_dir);
        Assert.Throws<TopicNotFoundException>(() => manager.GetPartition("nope", 0));
    }

    [Fact]
    public void TopicsSurviveRestart()
    {
        using (var manager = new TopicManager(_dir))
        {
            manager.CreateTopic("logs", partitions: 2);
            manager.GetPartition("logs", 0).Append(null, Bytes("x"), 1);
        }

        using (var reopened = new TopicManager(_dir))
        {
            Assert.Equal("logs", reopened.ListTopics().Single());
            Assert.Equal(2, reopened.PartitionCount("logs"));
            Assert.Equal(1, reopened.GetPartition("logs", 0).NextOffset);
        }
    }

    [Fact]
    public void ConflictingPartitionCount_Throws()
    {
        using var manager = new TopicManager(_dir);
        manager.CreateTopic("t", 2);
        Assert.Throws<InvalidOperationException>(() => manager.CreateTopic("t", 4));
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }
}
