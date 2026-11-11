using System.Buffers.Binary;
using Fld1.Manifest.Format;
using Fld1.Manifest.IO;
using Fld1.Manifest.Query;
using Xunit;

namespace Fld1.Manifest.Tests;

public class ManifestTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "fld-mf-" + Guid.NewGuid().ToString("N"));
    public ManifestTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private static byte[] Id(byte b)
    {
        var a = new byte[16];
        a[15] = b;
        return a;
    }

    [Fact]
    public void Empty_roundtrip()
    {
        var doc = ManifestDocument.Empty(0);
        var bytes = ManifestCodec.Encode(doc);
        var d2 = ManifestCodec.Decode(bytes);
        Assert.Equal(0ul, d2.Generation);
        Assert.Empty(d2.Runs);
        Assert.Empty(d2.Namespaces);
    }

    [Fact]
    public void Runs_and_ns_roundtrip()
    {
        var doc = new ManifestDocument
        {
            Generation = 3,
            LastFlushedCommitSeq = 42,
            CertifiedWatermark = new CertifiedWatermark(1000, 2),
            Namespaces = [1, 2],
            Runs =
            [
                new RunEntry
                {
                    RunId = 99, Level = 0, Ns = 1,
                    MinRec = Id(1), MaxRec = Id(9),
                    SeqLo = 1, SeqHi = 10
                }
            ],
            Retains = [],
            ResultClients = [],
            ProvenanceIndex = [],
            CoordinatorLeaseEpoch = 5,
            CoordinatorSessionId = Id(7)
        };
        var d2 = ManifestCodec.Decode(ManifestCodec.Encode(doc));
        Assert.Equal(3ul, d2.Generation);
        Assert.Equal(42ul, d2.LastFlushedCommitSeq);
        Assert.Equal(1000ul, d2.CertifiedWatermark.Wall);
        Assert.Equal(2u, d2.CertifiedWatermark.Logical);
        Assert.Equal(new uint[] { 1, 2 }, d2.Namespaces);
        Assert.Single(d2.Runs);
        Assert.Equal(99ul, d2.Runs[0].RunId);
        Assert.Equal(5ul, d2.CoordinatorLeaseEpoch);
        Assert.Equal(Id(7), d2.CoordinatorSessionId);
    }

    [Fact]
    public void Local_operation_receipts_roundtrip_in_manifest_extension()
    {
        var receipt = new LocalOperationReceiptEntry
        {
            NamespaceId = 1,
            ClientId = Id(0x41),
            OperationId = Id(0x42),
            ClientSequence = 1,
            Fingerprint = Enumerable.Range(0, 32).Select(value => (byte)value).ToArray(),
            CommitSeq = 17,
            Result = "original-response"u8.ToArray()
        };
        var document = ManifestDocument.Empty(2);
        var populated = new ManifestDocument
        {
            Generation = document.Generation,
            LastFlushedCommitSeq = 17,
            CertifiedWatermark = document.CertifiedWatermark,
            Namespaces = [1],
            Runs = [],
            Retains = [],
            ResultClients = [],
            ProvenanceIndex = [],
            LocalOperationReceipts = [receipt],
            LocalOperationClients =
            [
                new LocalOperationClientEntry
                {
                    NamespaceId = 1,
                    ClientId = receipt.ClientId,
                    LastCommittedSequence = 1,
                    AcknowledgedThroughSequence = 0
                }
            ],
            CoordinatorLeaseEpoch = document.CoordinatorLeaseEpoch,
            CoordinatorSessionId = document.CoordinatorSessionId
        };

        var decoded = ManifestCodec.Decode(ManifestCodec.Encode(populated));
        var recovered = Assert.Single(decoded.LocalOperationReceipts);
        Assert.Equal(1u, recovered.NamespaceId);
        Assert.Equal(receipt.ClientId, recovered.ClientId);
        Assert.Equal(receipt.OperationId, recovered.OperationId);
        Assert.Equal(receipt.Fingerprint, recovered.Fingerprint);
        Assert.Equal(receipt.CommitSeq, recovered.CommitSeq);
        Assert.Equal(receipt.Result, recovered.Result);
    }

    [Fact]
    public void Manifest_decoder_accepts_legacy_document_without_receipt_extension()
    {
        byte[] encoded = ManifestCodec.Encode(ManifestDocument.Empty(4));
        byte[] legacy = encoded.AsSpan(0, encoded.Length - 24).ToArray();
        BinaryPrimitives.WriteUInt32LittleEndian(
            legacy.AsSpan(8, 4), Fld1.Core.Hash.Hashes.Crc32C(legacy.AsSpan(12)));

        var decoded = ManifestCodec.Decode(legacy);
        Assert.Empty(decoded.LocalOperationReceipts);
        Assert.Equal(4ul, decoded.Generation);
    }

    [Fact]
    public void Corrupt_crc_throws()
    {
        var bytes = ManifestCodec.Encode(ManifestDocument.Empty(1));
        bytes[^1] ^= 0xFF;
        Assert.Throws<InvalidDataException>(() => ManifestCodec.Decode(bytes));
    }

    [Fact]
    public void Atomic_store_and_load()
    {
        var store = new AtomicManifestStore(_dir);
        Assert.Equal(0ul, store.Load().Generation);

        var doc = ManifestDocument.Empty(1);
        doc = new ManifestDocument
        {
            Generation = 1,
            LastFlushedCommitSeq = 7,
            CertifiedWatermark = new CertifiedWatermark(0, 0),
            Namespaces = [0],
            Runs = [],
            Retains = [],
            ResultClients = [],
            ProvenanceIndex = [],
            CoordinatorLeaseEpoch = 0,
            CoordinatorSessionId = new byte[16]
        };
        store.Store(doc);
        var loaded = store.Load();
        Assert.Equal(1ul, loaded.Generation);
        Assert.Equal(7ul, loaded.LastFlushedCommitSeq);
    }

    [Fact]
    public void Update_advances_generation()
    {
        var store = new AtomicManifestStore(_dir);
        store.Store(ManifestDocument.Empty(1));
        var next = store.Update(d => new ManifestDocument
        {
            Generation = d.Generation, // same — Update should bump
            LastFlushedCommitSeq = 100,
            CertifiedWatermark = d.CertifiedWatermark,
            Namespaces = d.Namespaces,
            Runs = d.Runs,
            Retains = d.Retains,
            ResultClients = d.ResultClients,
            ProvenanceIndex = d.ProvenanceIndex,
            CoordinatorLeaseEpoch = d.CoordinatorLeaseEpoch,
            CoordinatorSessionId = d.CoordinatorSessionId
        });
        Assert.Equal(2ul, next.Generation);
        Assert.Equal(100ul, next.LastFlushedCommitSeq);
        Assert.Equal(2ul, store.Load().Generation);
    }

    [Fact]
    public void Query_never_seen()
    {
        var m = ManifestDocument.Empty(0);
        var a = CommitResultQuery.Query(m, Id(1), 1);
        Assert.Equal(CommitResultStatus.UnknownNeverSeen, a.Status);
    }

    [Fact]
    public void Query_in_flight()
    {
        var m = new ManifestDocument
        {
            Generation = 1,
            LastFlushedCommitSeq = 0,
            CertifiedWatermark = new CertifiedWatermark(0, 0),
            Namespaces = [],
            Runs = [],
            Retains = [],
            ResultClients =
            [
                new ResultClient
                {
                    ClientInstanceId = Id(1),
                    SeqLo = 1, SeqHi = 5,
                    FinalizedThroughSeq = 3,
                    FloorSeq = 1,
                    AcceptedBitmap = new byte[1]
                }
            ],
            ProvenanceIndex = [],
            CoordinatorLeaseEpoch = 0,
            CoordinatorSessionId = new byte[16]
        };
        var a = CommitResultQuery.Query(m, Id(1), 4);
        Assert.Equal(CommitResultStatus.UnknownInFlight, a.Status);
    }

    [Fact]
    public void Query_committed_from_provenance()
    {
        var m = new ManifestDocument
        {
            Generation = 1,
            LastFlushedCommitSeq = 10,
            CertifiedWatermark = new CertifiedWatermark(0, 0),
            Namespaces = [],
            Runs = [],
            Retains = [],
            ResultClients =
            [
                new ResultClient
                {
                    ClientInstanceId = Id(1),
                    SeqLo = 1, SeqHi = 10,
                    FinalizedThroughSeq = 10,
                    FloorSeq = 1,
                    AcceptedBitmap = new byte[] { 0xFF, 0xFF }
                }
            ],
            ProvenanceIndex =
            [
                new ProvenanceIndexEntry
                {
                    ClientInstanceId = Id(1),
                    RequestSeq = 2,
                    Outcome = ManifestOutcome.Committed,
                    CommitSeq = 99,
                    ErrorCode = 0
                }
            ],
            CoordinatorLeaseEpoch = 0,
            CoordinatorSessionId = new byte[16]
        };
        var a = CommitResultQuery.Query(m, Id(1), 2);
        Assert.Equal(CommitResultStatus.Committed, a.Status);
        Assert.Equal(99ul, a.CommitSeq);
    }

    [Fact]
    public void Deterministic_encode()
    {
        var doc = ManifestDocument.Empty(5);
        Assert.True(ManifestCodec.Encode(doc).AsSpan().SequenceEqual(ManifestCodec.Encode(doc)));
    }
}
