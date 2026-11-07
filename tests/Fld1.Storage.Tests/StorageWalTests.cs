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