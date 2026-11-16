using Fld1.Engine.Db;
using Xunit;

namespace Fld1.Engine.Db.Tests;

public sealed class FldTombstoneGcApprovalSpoolTests
{
    [Fact]
    public void Approval_spool_is_strictly_ordered_and_matches_exact_current_identity()
    {
        string directory = Path.Combine(Path.GetTempPath(), "fld-gc-spool-" + Guid.NewGuid().ToString("N"));
        Guid token = Guid.NewGuid();
        var first = Candidate(0x11, 7, 0x31);
        var second = Candidate(0x22, 8, 0x32);

        try
        {
            using var spool = new FldTombstoneGcApprovalSpool(directory, token);
            spool.Append(new[] { first });
            spool.Append(new[] { second });
            byte[] digest = spool.Seal();
            Assert.Equal(32, digest.Length);

            using var reader = spool.OpenReader();
            Assert.False(reader.IsApproved(Candidate(0x10, 1, 0x30)));
            Assert.True(reader.IsApproved(first));
            Assert.False(reader.IsApproved(Candidate(0x11, 9, 0x31)));
            Assert.True(reader.IsApproved(second));
            Assert.False(reader.IsApproved(Candidate(0x33, 9, 0x33)));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Approval_spool_rejects_duplicate_or_out_of_order_keys()
    {
        string directory = Path.Combine(Path.GetTempPath(), "fld-gc-spool-order-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var spool = new FldTombstoneGcApprovalSpool(directory, Guid.NewGuid());
            FldTombstoneGcCandidate later = Candidate(0x22, 8, 0x32);
            FldTombstoneGcCandidate earlier = Candidate(0x11, 7, 0x31);
            spool.Append(new[] { later });
            Assert.Throws<InvalidDataException>(() => spool.Append(new[] { earlier }));
            Assert.Throws<InvalidDataException>(() => spool.Append(new[] { later }));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Approval_spool_detects_changes_after_seal()
    {
        string directory = Path.Combine(Path.GetTempPath(), "fld-gc-spool-digest-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var spool = new FldTombstoneGcApprovalSpool(directory, Guid.NewGuid());
            spool.Append(new[] { Candidate(0x11, 7, 0x31) });
            spool.Seal();

            using (var stream = new FileStream(spool.PathName, FileMode.Open, FileAccess.Write, FileShare.Read))
            {
                stream.Position = 0;
                stream.WriteByte(0xFF);
                stream.Flush(flushToDisk: true);
            }

            Assert.Throws<InvalidDataException>(() => spool.OpenReader());
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    private static FldTombstoneGcCandidate Candidate(byte record, ushort field, byte change)
    {
        byte[] recordId = new byte[16];
        byte[] changeId = new byte[16];
        byte[] authorId = new byte[16];
        recordId[0] = record;
        changeId[0] = change;
        authorId[0] = 0x55;
        return new FldTombstoneGcCandidate(
            recordId, field, changeId, authorId,
            hlcWall: 100 + record, hlcLogical: 1,
            originAuthorEpoch: 2, originSequence: 3,
            isRecordTombstone: false, isFieldTombstone: true);
    }
}
