using Fld1.Wal.Codec;
using Fld1.Wal.Format;
using Fld1.Wal.IO;
using Fld1.Wal.Recovery;
using Xunit;

namespace Fld1.Wal.Tests;

public class WalBasicTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "fld-wal-" + Guid.NewGuid().ToString("N"));

    public WalBasicTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { /* ignore */ } }

    private string PathOf(string name) => Path.Combine(_dir, name);

    private static byte[] Id(byte b)
    {
        var a = new byte[16];
        a[15] = b;
        return a;
    }

    [Fact]
    public void Header_magic_and_crc()
    {
        Span<byte> h = stackalloc byte[64];
        WalHeader.Write(h, namespaceId: 7, firstCommitSeq: 1);
        Assert.True(WalHeader.CheckMagic(h));
        Assert.True(WalHeader.VerifyCrc(h));
        Assert.Equal(7ul, WalHeader.ReadNamespaceId(h));
        Assert.Equal(1ul, WalHeader.ReadFirstCommitSeq(h));
        Assert.Equal(WalConstants.WalVersion, WalHeader.ReadVersion(h));
    }

    [Fact]
    public void Empty_wal_recover_ok()
    {
        var path = PathOf("empty.log");
        using (WalWriter.Create(path, 1, firstCommitSeq: 1)) { }
        var r = WalRecovery.Recover(path);
        Assert.Equal(WalRecoveryStatus.Ok, r.Status);
        Assert.Empty(r.Records);
        Assert.Equal(WalHeader.Size, r.ValidEndOffset);
    }

    [Fact]
    public void Append_and_recover_group_commit()
    {
        var path = PathOf("gc.log");
        var payload = GroupCommitCodec.Encode(new GroupCommitPayload
        {
            Mutations =
            [
                new WalMutation
                {
                    ClientInstanceId = Id(1),
                    RequestSeq = 10,
                    Ns = 0,
                    Fields =
                    [
                        new WalFieldMutation
                        {
                            RecordId = Id(9),
                            FieldId = 1,
                            OpKind = 1,
                            KeyEpoch = 0,
                            Wall = 100,
                            Logical = 0,
                            AuthorNodeId = Id(1),
                            Value = "hello"u8.ToArray()
                        }
                    ]
                }
            ],
            Results =
            [
                new WalResult
                {
                    ClientInstanceId = Id(1),
                    RequestSeq = 10,
                    Status = WalConstants.StatusCommitted,
                    ErrorCode = 0,
                    CommitSeq = 1, // will be overwritten conceptually; stored in payload as given
                    ProvenanceIdx = 0xFFFFFFFF
                }
            ],
            Provenance = []
        });

        ulong seq;
        using (var w = WalWriter.Create(path, namespaceId: 0, firstCommitSeq: 1))
        {
            seq = w.Append(WalConstants.KindGroupCommit, payload);
            Assert.Equal(1ul, seq);
            w.Flush();
        }

        var r = WalRecovery.Recover(path);
        Assert.Equal(WalRecoveryStatus.Ok, r.Status);
        Assert.Single(r.Records);
        Assert.Equal(1ul, r.Records[0].CommitSeq);
        Assert.Equal(WalConstants.KindGroupCommit, r.Records[0].Kind);

        var decoded = GroupCommitCodec.Decode(r.Records[0].Payload);
        Assert.Single(decoded.Mutations);
        Assert.Equal("hello"u8.ToArray(), decoded.Mutations[0].Fields[0].Value);
        Assert.Single(decoded.Results);
    }

    [Fact]
    public void Multiple_records_monotonic_seq()
    {
        var path = PathOf("multi.log");
        using (var w = WalWriter.Create(path, 0, 1))
        {
            w.Append(WalConstants.KindGroupCommit, GroupCommitCodec.Encode(EmptyPayload()));
            w.Append(WalConstants.KindGroupCommit, GroupCommitCodec.Encode(EmptyPayload()));
            w.Append(WalConstants.KindNsCreate, ReadOnlySpan<byte>.Empty);
            w.Flush();
        }

        var r = WalRecovery.Recover(path);
        Assert.Equal(WalRecoveryStatus.Ok, r.Status);
        Assert.Equal(3, r.Records.Count);
        Assert.Equal(1ul, r.Records[0].CommitSeq);
        Assert.Equal(2ul, r.Records[1].CommitSeq);
        Assert.Equal(3ul, r.Records[2].CommitSeq);
        Assert.Equal(WalConstants.KindNsCreate, r.Records[2].Kind);
    }

    [Fact]
    public void Codec_roundtrip_empty_group()
    {
        var p = EmptyPayload();
        var bytes = GroupCommitCodec.Encode(p);
        var d = GroupCommitCodec.Decode(bytes);
        Assert.Empty(d.Mutations);
        Assert.Empty(d.Results);
        Assert.Empty(d.Provenance);
    }

    [Fact]
    public void Torn_tail_truncates()
    {
        var path = PathOf("torn.log");
        using (var w = WalWriter.Create(path, 0, 1))
        {
            w.Append(WalConstants.KindGroupCommit, GroupCommitCodec.Encode(EmptyPayload()));
            w.Flush();
        }
        // Append garbage short tail
        using (var fs = new FileStream(path, FileMode.Append))
        {
            fs.Write(new byte[] { 0x01, 0x02, 0x03 });
        }

        var r = WalRecovery.Recover(path);
        Assert.Equal(WalRecoveryStatus.TruncatedTorn, r.Status);
        Assert.Single(r.Records);
        Assert.True(r.ValidEndOffset > WalHeader.Size);
    }

    [Fact]
    public void Full_record_crc_corruption_is_not_treated_as_torn_tail()
    {
        var path = PathOf("corrupt-tail.log");
        using (var writer = WalWriter.Create(path, namespaceId: 0, firstCommitSeq: 1))
        {
            writer.Append(WalConstants.KindGroupCommit, GroupCommitCodec.Encode(EmptyPayload()));
            writer.Flush();
        }

        byte[] bytes = File.ReadAllBytes(path);
        bytes[WalHeader.Size + WalEnvelope.Size] ^= 0x40; // mutate payload, retain valid envelope
        File.WriteAllBytes(path, bytes);

        var recovered = WalRecovery.Recover(path);
        Assert.Equal(WalRecoveryStatus.CorruptTailAmbiguous, recovered.Status);
        Assert.Equal((long)bytes.Length, new FileInfo(path).Length);
    }

    [Fact]
    public void Corrupt_header_stops()
    {
        var path = PathOf("badhdr.log");
        using (var w = WalWriter.Create(path, 0, 1)) { }
        var bytes = File.ReadAllBytes(path);
        bytes[0] = (byte)'X';
        File.WriteAllBytes(path, bytes);

        var r = WalRecovery.Recover(path);
        Assert.Equal(WalRecoveryStatus.CorruptHeader, r.Status);
    }

    [Fact]
    public void Record_alignment_8()
    {
        var path = PathOf("align.log");
        // payload length 1 → raw = 25 → aligned 32
        using (var w = WalWriter.Create(path, 0, 1))
        {
            w.Append(WalConstants.KindGroupCommit, new byte[] { 0xAB });
            w.Flush();
        }
        var r = WalRecovery.Recover(path);
        Assert.Equal(WalRecoveryStatus.Ok, r.Status);
        Assert.Equal(0, r.ValidEndOffset % 8);
    }

    private static GroupCommitPayload EmptyPayload() => new()
    {
        Mutations = [],
        Results = [],
        Provenance = []
    };
}
