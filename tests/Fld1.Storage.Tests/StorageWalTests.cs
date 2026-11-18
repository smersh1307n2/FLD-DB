using Fld1.Storage.Wal.Format;
using Fld1.Storage.Wal.IO;
using Fld1.Storage.Wal.Recovery;
using Xunit;

namespace Fld1.Storage.Tests;

public sealed class StorageWalTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "fld-storage-wal-" + Guid.NewGuid().ToString("N"));
    public StorageWalTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    [Fact]
    public void Generic_record_kinds_and_payloads_round_trip_without_domain_decoder()
    {
        string path = WalManager.SegmentPath(_dir, 7, 0);
        using (var writer = WalWriter.Create(path, partitionId: 7))
        {
            Assert.Equal(1UL, writer.Append(0x71, "opaque metadata payload"u8));
            writer.Flush();
        }

        var recovered = WalRecovery.Recover(path);
        Assert.Equal(WalRecoveryStatus.Ok, recovered.Status);
        var record = Assert.Single(recovered.Records);
        Assert.Equal((byte)0x71, record.Kind);
        Assert.Equal("opaque metadata payload"u8.ToArray(), record.Payload);
    }

    [Fact]
    public void Manager_reopens_rotated_segments_and_preserves_global_commit_sequence()
    {
        var (manager, _) = WalManager.Open(_dir, 9, rotateAfterBytes: 0);
        using (manager)
        {
            Assert.Equal(1UL, manager.Append(0x51, "first"u8));
            manager.Flush();
            manager.Rotate();
            Assert.Equal(2UL, manager.Append(0x52, "second"u8));
            manager.Flush();
        }

        var (reopened, records) = WalManager.Open(_dir, 9, rotateAfterBytes: 0);
        using (reopened)
        {
            Assert.Equal(new ulong[] { 1, 2 }, records.Select(x => x.CommitSeq));
            Assert.Equal(new byte[] { 0x51, 0x52 }, records.Select(x => x.Kind));
            Assert.Equal(3UL, reopened.NextCommitSeq);
        }
    }

    [Fact]
    public void Streaming_recovery_visits_records_without_materializing_the_record_list()
    {
        string path = WalManager.SegmentPath(_dir, 13, 0);
        using (var writer = WalWriter.Create(path, partitionId: 13))
        {
            writer.Append(0x61, "first"u8);
            writer.Append(0x62, "second"u8);
            writer.Append(0x63, "third"u8);
            writer.Flush();
        }

        var visited = new List<(ulong Seq, byte Kind, byte[] Payload)>();
        WalRecoveryResult recovered = WalRecovery.RecoverStreaming(path,
            record => visited.Add((record.CommitSeq, record.Kind, record.Payload.ToArray())));

        Assert.Equal(WalRecoveryStatus.Ok, recovered.Status);
        Assert.Equal(3, recovered.RecordCount);
        Assert.Empty(recovered.Records);
        Assert.Equal(new ulong[] { 1, 2, 3 }, visited.Select(record => record.Seq));
        Assert.Equal(new byte[] { 0x61, 0x62, 0x63 }, visited.Select(record => record.Kind));
        Assert.Equal(new[] { "first", "second", "third" },
            visited.Select(record => System.Text.Encoding.UTF8.GetString(record.Payload)));
    }

    [Fact]
    public void Streaming_manager_replays_rotated_segments_in_commit_sequence_order()
    {
        var (manager, _) = WalManager.Open(_dir, 19, rotateAfterBytes: 0);
        using (manager)
        {
            Assert.Equal(1UL, manager.Append(0x71, "one"u8));
            manager.Flush();
            manager.Rotate();
            Assert.Equal(2UL, manager.Append(0x72, "two"u8));
            manager.Flush();
        }

        var (reopened, recovery) = WalManager.OpenStreaming(_dir, 19, rotateAfterBytes: 0);
        using (reopened)
        {
            Assert.True(recovery.HasRecords);
            Assert.Equal(3UL, recovery.NextCommitSeq);
            var visited = new List<ulong>();
            recovery.Replay(record => visited.Add(record.CommitSeq));
            Assert.Equal(new ulong[] { 1, 2 }, visited);
        }
    }

    [Fact]
    public void Recovery_rejects_reserved_zero_kind()
    {
        string path = WalManager.SegmentPath(_dir, 11, 0);
        using (var writer = WalWriter.Create(path, partitionId: 11))
        {
            writer.Append(0x01, "valid"u8);
            writer.Flush();
        }

        byte[] bytes = File.ReadAllBytes(path);
        bytes[WalHeader.Size + WalEnvelope.OffKind] = 0;
        File.WriteAllBytes(path, bytes);
        Assert.Equal(WalRecoveryStatus.CorruptEnvelope, WalRecovery.Recover(path).Status);
    }
}