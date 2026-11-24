using Mq.Core;
using Record = Mq.Core.Record;

namespace Mq.Tests;

public sealed class SegmentedLogTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "mq-tests", Guid.NewGuid().ToString("N"));

    private static byte[] Bytes(string s) => System.Text.Encoding.UTF8.GetBytes(s);

    [Fact]
    public void Append_AssignsSequentialOffsets_AndReadReturnsRecords()
    {
        using var log = new SegmentedLog(_dir);
        long o0 = log.Append(null, Bytes("first"), 1000);
        long o1 = log.Append(Bytes("k"), Bytes("second"), 2000);

        Assert.Equal(0, o0);
        Assert.Equal(1, o1);

        IReadOnlyList<Record> records = log.Read(0);
        Assert.Equal(2, records.Count);
        Assert.Equal("first", System.Text.Encoding.UTF8.GetString(records[0].Value));
        Assert.Equal("second", System.Text.Encoding.UTF8.GetString(records[1].Value));
        Assert.Equal(Bytes("k"), records[1].Key);
        Assert.Equal(2000, records[1].TimestampMs);
    }

    [Fact]
    public void Read_FromMiddleOffset_SkipsEarlierRecords()
    {
        using var log = new SegmentedLog(_dir);
        for (int i = 0; i < 50; i++)
            log.Append(null, Bytes($"msg-{i}"), i);

        IReadOnlyList<Record> records = log.Read(37);
        Assert.Equal(13, records.Count);
        Assert.Equal(37, records[0].Offset);
        Assert.Equal("msg-37", System.Text.Encoding.UTF8.GetString(records[0].Value));
    }

    [Fact]
    public void SegmentRolling_ReadsSpanSegmentBoundaries()
    {
        var options = new LogOptions { MaxSegmentBytes = 256, IndexIntervalBytes = 16 };
        using (var log = new SegmentedLog(_dir, options))
        {
            for (int i = 0; i < 100; i++)
                log.Append(null, Bytes($"payload-{i:D3}-xxxxxxxxxxxxxxxxxxxxxxxxxxxx"), i);
            Assert.True(log.Segments.Count > 1, "expected multiple segments");

            IReadOnlyList<Record> records = log.Read(0, maxRecords: 200);
            Assert.Equal(100, records.Count);
            for (int i = 0; i < 100; i++)
                Assert.Equal(i, records[i].Offset);
        }

        // Reopen: offsets and content survive a restart.
        using (var reopened = new SegmentedLog(_dir, options))
        {
            Assert.Equal(100, reopened.NextOffset);
            IReadOnlyList<Record> records = reopened.Read(90, maxRecords: 20);
            Assert.Equal(10, records.Count);
            Assert.Equal(90, records[0].Offset);
        }
    }

    [Fact]
    public void CorruptedRecord_ThrowsCorruptedRecordException()
    {
        var record = new Record(0, 123, null, Bytes("hello"));
        byte[] encoded = RecordCodec.Encode(record);
        encoded[^6] ^= 0xFF; // flip a bit inside the value, before the CRC

        Assert.Throws<CorruptedRecordException>(() => RecordCodec.Decode(encoded, out _));
    }

    [Fact]
    public void TornTailWrite_IsTruncatedOnRecovery()
    {
        var options = new LogOptions { MaxSegmentBytes = long.MaxValue };
        using (var log = new SegmentedLog(_dir, options))
        {
            log.Append(null, Bytes("good-1"), 1);
            log.Append(null, Bytes("good-2"), 2);
            log.Flush();
        }

        // Simulate a crash mid-write: append garbage bytes to the segment tail.
        string logFile = Directory.GetFiles(_dir, "*.log").Single();
        using (var fs = new FileStream(logFile, FileMode.Append, FileAccess.Write))
            fs.Write(new byte[] { 0x00, 0x00, 0x00, 0x40, 0xAB });

        using (var reopened = new SegmentedLog(_dir, options))
        {
            Assert.Equal(2, reopened.NextOffset);
            IReadOnlyList<Record> records = reopened.Read(0);
            Assert.Equal(2, records.Count);
        }
    }

    [Fact]
    public void RetentionBySize_DeletesOldestSegments_ButNeverActive()
    {
        var options = new LogOptions { MaxSegmentBytes = 200, RetentionBytes = 400 };
        using var log = new SegmentedLog(_dir, options);
        for (int i = 0; i < 60; i++)
        {
            log.Append(null, Bytes($"retention-payload-{i:D3}-yyyyyyyyyyyyyyyy"), i);
            log.ApplyRetention(nowMs: i);
        }

        Assert.True(log.Segments.Count >= 1);
        Assert.True(log.Segments.Sum(s => s.SizeBytes) <= 400 + 256);
        Assert.True(log.BaseOffset > 0, "oldest segments should have been deleted");
        // Records that remain are still readable and contiguous.
        IReadOnlyList<Record> remaining = log.Read(log.BaseOffset, maxRecords: 1000);
        Assert.Equal(log.NextOffset - log.BaseOffset, remaining.Count);
    }

    [Fact]
    public void NullKey_RoundTrips_AsNull()
    {
        var record = new Record(7, 99, null, Bytes("v"));
        byte[] encoded = RecordCodec.Encode(record);
        Record decoded = RecordCodec.Decode(encoded, out int consumed);
        Assert.Null(decoded.Key);
        Assert.Equal(7, decoded.Offset);
        Assert.Equal(encoded.Length, consumed);
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }
}
