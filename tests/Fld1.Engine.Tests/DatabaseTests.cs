using Fld1.Core.Uuid;
using Fld1.Engine.Db;
using Xunit;

namespace Fld1.Engine.Tests;

public class DatabaseTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "fld-db-" + Guid.NewGuid().ToString("N"));
    public DatabaseTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private static byte[] Rec(byte b)
    {
        var id = new byte[16];
        id[0] = b;
        id[6] = 0x70;
        id[8] = 0x80;
        return id;
    }

    private static Guid RunBlobId(ulong runId)
    {
        Span<byte> id = stackalloc byte[16];
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(id, runId);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(id.Slice(8), 0x314E5552);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(id.Slice(12), 0x31444C46);
        return new Guid(id);
    }

    private string RunBlobPath(ulong runId)
        => Path.Combine(_dir, "storage-core", "blobs", Convert.ToHexString("FLDRUNB1"u8),
            RunBlobId(runId).ToString("N"), "content.blob");

    [Fact]
    public void Put_get_from_overlay()
    {
        using var db = FldDatabase.Open(_dir, namespaceId: 0);
        var rec = Rec(1);
        db.Put(rec, 10, "hello"u8);
        Assert.True(db.TryGetBytes(rec, 10, out var v));
        Assert.Equal("hello"u8.ToArray(), v);
    }

    [Fact]
    public void Commit_writes_wal()
    {
        using var db = FldDatabase.Open(_dir, 0);
        db.Put(Rec(2), 1, "x"u8);
        ulong seq = db.Commit();
        Assert.True(seq >= 1);
        Assert.True(File.Exists(Path.Combine(_dir, "wal", "ns_0.0.log")));
    }

    [Fact]
    public void Checkpoint_run_and_get()
    {
        using var db = FldDatabase.Open(_dir, 0);
        var rec = Rec(3);
        db.Put(rec, 5, "sealed"u8);
        db.Commit();
        ulong runId = db.Checkpoint();
        Assert.True(runId >= 1);
        Assert.Equal(0, db.OverlayCount);
        Assert.True(db.TryGetBytes(rec, 5, out var v));
        Assert.Equal("sealed"u8.ToArray(), v);
        Assert.False(File.Exists(Path.Combine(_dir, "runs", $"{runId}.fld1")));
        Assert.True(File.Exists(RunBlobPath(runId)));
    }

    [Fact]
    public void Reopen_reads_run()
    {
        var rec = Rec(4);
        ulong runId;
        using (var db = FldDatabase.Open(_dir, 0))
        {
            db.Put(rec, 1, "persist"u8);
            db.Commit();
            runId = db.Checkpoint();
        }
        Assert.False(File.Exists(Path.Combine(_dir, "runs", $"{runId}.fld1")));
        Assert.True(File.Exists(RunBlobPath(runId)));
        using (var db2 = FldDatabase.Open(_dir, 0))
        {
            Assert.True(db2.TryGetBytes(rec, 1, out var v));
            Assert.Equal("persist"u8.ToArray(), v);
            Assert.False(File.Exists(Path.Combine(_dir, "runs", $"{runId}.fld1")));
        }
    }

    [Fact]
    public void Run_span_remains_valid_after_compaction_retires_its_mapping()
    {
        var rec = Rec(0x31);
        using var db = FldDatabase.Open(_dir, 0);

        db.Put(rec, 7, "first-generation"u8);
        db.Commit();
        ulong firstRun = db.Checkpoint();
        Assert.True(firstRun > 0);
        Assert.True(db.TryGet(rec, 7, out ReadOnlySpan<byte> retainedValue));

        db.Put(rec, 7, "second-generation"u8);
        db.Commit();
        db.Checkpoint();
        db.Compact();

        // A RunReader slice can escape TryGet. Compaction must retire the old
        // mapping instead of unmapping memory that a caller may still reference.
        Assert.Equal("first-generation"u8.ToArray(), retainedValue.ToArray());
        Assert.True(db.TryGetBytes(rec, 7, out byte[]? current));
        Assert.Equal("second-generation"u8.ToArray(), current);
    }

    [Fact]
    public void Reopen_uses_authoritative_blob_and_removes_stale_legacy_mirror()
    {
        var rec = Rec(0x32);
        ulong runId;
        using (var db = FldDatabase.Open(_dir, 0))
        {
            db.Put(rec, 8, "authoritative"u8);
            db.Commit();
            runId = db.Checkpoint();
        }

        byte[] author = new byte[16];
        author[15] = 0x22;
        byte[] staleMirror = new Fld1.Run.Builder.RunBuilder(0)
            .Add(rec, 8, "stale-but-valid"u8, wall: 9999, logical: 0, author)
            .Build();
        string mirrorPath = Path.Combine(_dir, "runs", $"{runId}.fld1");
        File.WriteAllBytes(mirrorPath, staleMirror);

        using (var reopened = FldDatabase.Open(_dir, 0))
        {
            Assert.True(reopened.TryGetBytes(rec, 8, out byte[]? value));
            Assert.Equal("authoritative"u8.ToArray(), value);
        }

        Assert.False(File.Exists(mirrorPath),
            "The redundant legacy mirror must be removed after the authoritative Storage blob is verified.");
        Assert.True(File.Exists(RunBlobPath(runId)));
    }

    [Fact]
    public void Reopen_keeps_legacy_run_fallback_when_storage_blob_is_absent()
    {
        var rec = Rec(0x37);
        ulong runId;
        using (var db = FldDatabase.Open(_dir, 0))
        {
            db.Put(rec, 1, "legacy"u8);
            db.Commit();
            runId = db.Checkpoint();
        }

        using (var storage = Fld1.Storage.Engine.StorageDatabase.Open(
            Path.Combine(_dir, "storage-core"), partitionId: 0))
        {
            var blobs = new Fld1.Storage.Engine.StorageBlobStore(storage, "FLDRUNB1"u8);
            Assert.True(blobs.DeleteBlob(RunBlobId(runId)));
        }

        byte[] author = new byte[16];
        author[15] = 0x37;
        byte[] legacyRun = new Fld1.Run.Builder.RunBuilder(0)
            .Add(rec, 1, "legacy"u8, wall: 1, logical: 0, author)
            .Build();
        string legacyPath = Path.Combine(_dir, "runs", $"{runId}.fld1");
        File.WriteAllBytes(legacyPath, legacyRun);

        using var reopened = FldDatabase.Open(_dir, 0);
        Assert.True(reopened.TryGetBytes(rec, 1, out byte[]? value));
        Assert.Equal("legacy"u8.ToArray(), value);
        Assert.True(File.Exists(legacyPath),
            "A pre-Storage legacy Run must remain available when no authoritative blob exists.");
    }

    [Fact]
    public void Overlay_wins_over_run()
    {
        var rec = Rec(5);
        using var db = FldDatabase.Open(_dir, 0);
        db.Put(rec, 1, "old"u8);
        db.Commit();
        db.Checkpoint();
        db.Put(rec, 1, "new"u8);
        Assert.True(db.TryGetBytes(rec, 1, out var v));
        Assert.Equal("new"u8.ToArray(), v);
    }

    [Fact]
    public void Multi_field_checkpoint()
    {
        using var db = FldDatabase.Open(_dir, 0);
        var rec = Rec(6);
        db.Put(rec, 1, "a"u8);
        db.Put(rec, 2, "b"u8);
        db.Commit();
        db.Checkpoint();
        Assert.True(db.TryGetBytes(rec, 1, out var a) && a!.SequenceEqual("a"u8.ToArray()));
        Assert.True(db.TryGetBytes(rec, 2, out var b) && b!.SequenceEqual("b"u8.ToArray()));
    }
}
