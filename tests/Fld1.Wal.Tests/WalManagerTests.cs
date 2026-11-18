using Fld1.Wal.Codec;
using Fld1.Wal.Format;
using Fld1.Wal.IO;
using Xunit;

namespace Fld1.Wal.Tests;

public class WalManagerTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "fld-wm-" + Guid.NewGuid().ToString("N"));
    public WalManagerTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private static byte[] EmptyPayload()
        => GroupCommitCodec.Encode(new GroupCommitPayload
        {
            Mutations = [], Results = [], Provenance = []
        });

    [Fact]
    public void Rotate_creates_new_segment()
    {
        var (m, _) = WalManager.Open(_dir, 1, rotateAfterBytes: 0); // manual rotate only
        using (m)
        {
            m.Append(WalConstants.KindGroupCommit, EmptyPayload());
            m.Flush();
            Assert.Equal(0, m.ActiveSegment);
            m.Rotate();
            Assert.Equal(1, m.ActiveSegment);
            m.Append(WalConstants.KindGroupCommit, EmptyPayload());
            m.Flush();
        }
        Assert.True(File.Exists(WalManager.SegmentPath(_dir, 1, 0)));
        Assert.True(File.Exists(WalManager.SegmentPath(_dir, 1, 1)));
    }

    [Fact]
    public void Reopen_replays_all_segments()
    {
        {
            var (m, _) = WalManager.Open(_dir, 2, rotateAfterBytes: 0);
            using (m)
            {
                m.Append(WalConstants.KindGroupCommit, EmptyPayload());
                m.Flush();
                m.Rotate();
                m.Append(WalConstants.KindGroupCommit, EmptyPayload());
                m.Flush();
            }
        }
        var (m2, records) = WalManager.Open(_dir, 2, rotateAfterBytes: 0);
        using (m2)
        {
            Assert.Equal(2, records.Count);
            Assert.Equal(1ul, records[0].CommitSeq);
            Assert.Equal(2ul, records[1].CommitSeq);
            Assert.Equal(3ul, m2.NextCommitSeq);
        }
    }

    [Fact]
    public void Open_refuses_full_crc_corruption_without_truncating()
    {
        string path = WalManager.SegmentPath(_dir, 5, 0);
        using (var writer = WalWriter.Create(path, namespaceId: 5, firstCommitSeq: 1))
        {
            writer.Append(WalConstants.KindGroupCommit, EmptyPayload());
            writer.Flush();
        }

        byte[] bytes = File.ReadAllBytes(path);
        bytes[WalHeader.Size + WalEnvelope.Size] ^= 0x20;
        File.WriteAllBytes(path, bytes);

        Assert.Throws<InvalidDataException>(() => WalManager.Open(_dir, 5, rotateAfterBytes: 0));
        Assert.Equal((long)bytes.Length, new FileInfo(path).Length);
    }

    [Fact]
    public void Reopen_after_truncating_prefix_keeps_global_commit_sequence()
    {
        string path0 = WalManager.SegmentPath(_dir, 6, 0);
        string path1 = WalManager.SegmentPath(_dir, 6, 1);

        var (manager, _) = WalManager.Open(_dir, 6, rotateAfterBytes: 0);
        using (manager)
        {
            Assert.Equal(1ul, manager.Append(WalConstants.KindGroupCommit, EmptyPayload()));
            manager.Flush();
            manager.Rotate();
            Assert.Equal(2ul, manager.Append(WalConstants.KindGroupCommit, EmptyPayload()));
            manager.Flush();
            Assert.Equal(1, manager.TruncateSealedSegments(sealedThroughSeq: 1));
        }

        Assert.False(File.Exists(path0));
        Assert.True(File.Exists(path1));

        var (reopened, records) = WalManager.Open(_dir, 6, rotateAfterBytes: 0);
        using (reopened)
        {
            Assert.Single(records);
            Assert.Equal(2ul, records[0].CommitSeq);
            Assert.Equal(3ul, reopened.NextCommitSeq);
            Assert.Equal(3ul, reopened.Append(WalConstants.KindGroupCommit, EmptyPayload()));
            reopened.Flush();
        }
    }

    [Fact]
    public void Truncate_drops_sealed_segments()
    {
        var (m, _) = WalManager.Open(_dir, 3, rotateAfterBytes: 0);
        using (m)
        {
            m.Append(WalConstants.KindGroupCommit, EmptyPayload());
            m.Flush();
            m.Rotate();
            m.Append(WalConstants.KindGroupCommit, EmptyPayload());
            m.Flush();
            int n = m.TruncateSealedSegments(sealedThroughSeq: 1);
            Assert.True(n >= 1);
            Assert.False(File.Exists(WalManager.SegmentPath(_dir, 3, 0)));
            Assert.True(File.Exists(WalManager.SegmentPath(_dir, 3, 1)));
        }
    }
}
