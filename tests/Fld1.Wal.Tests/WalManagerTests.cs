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
