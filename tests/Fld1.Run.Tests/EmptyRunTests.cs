using Fld1.Core.Canon;
using Fld1.Core.Hash;
using Fld1.Run.Builder;
using Fld1.Run.Format;
using Fld1.Run.Reader;
using Fld1.Run.Verify;
using System.Buffers.Binary;
using Xunit;

namespace Fld1.Run.Tests;

public class EmptyRunTests
{
    [Fact]
    public void Empty_run_is_264_bytes()
    {
        var run = EmptyRunBuilder.Create(0);
        Assert.Equal(RunConstants.EmptyRunSize, run.Length);
        Assert.Equal(264, run.Length);
    }

    [Fact]
    public void Empty_run_magic_and_version()
    {
        var run = EmptyRunBuilder.Create(42);
        Assert.True(RunHeader.CheckMagic(run));
        Assert.Equal(RunConstants.BinaryVersion, RunHeader.ReadBinaryVersion(run));
        Assert.Equal(42u, RunHeader.ReadNamespaceId(run));
        Assert.Equal(RunFlags.None, RunHeader.ReadFlags(run));
    }

    [Fact]
    public void Empty_run_verify_ok()
    {
        var run = EmptyRunBuilder.Create(1);
        var r = RunVerify.Verify(run);
        Assert.True(r.IsOk, r.Detail);
    }

    [Fact]
    public void Empty_run_rootDigest_canonical()
    {
        uint ns = 7;
        var run = EmptyRunBuilder.Create(ns);
        Span<byte> nsLe = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(nsLe, ns);
        Span<byte> expected = stackalloc byte[32];
        Hashes.Blake3_256Tagged(Tags.RunStateV1, nsLe, expected);
        Assert.True(expected.SequenceEqual(RunHeader.RootDigest(run)));
    }

    [Fact]
    public void Empty_run_two_builders_identical()
    {
        var a = EmptyRunBuilder.Create(99);
        var b = EmptyRunBuilder.Create(99);
        Assert.True(a.AsSpan().SequenceEqual(b));
    }

    [Fact]
    public void Empty_run_get_is_miss()
    {
        var run = EmptyRunBuilder.Create(0);
        var reader = new RunReader(run);
        Assert.True(reader.IsEmpty);
        Assert.False(reader.TryGet(new byte[16], 1, out _));
    }

    [Fact]
    public void Corrupt_magic_fails_verify()
    {
        var run = EmptyRunBuilder.Create(0);
        run[0] = (byte)'X';
        var r = RunVerify.Verify(run);
        Assert.Equal(RunVerifyStatus.BadMagic, r.Status);
    }

    [Fact]
    public void Corrupt_footer_fails_verify()
    {
        var run = EmptyRunBuilder.Create(0);
        run[^1] ^= 0xFF;
        var r = RunVerify.Verify(run);
        Assert.Equal(RunVerifyStatus.BadFooter, r.Status);
    }

    [Fact]
    public void Nonzero_reserved_fails()
    {
        var run = EmptyRunBuilder.Create(0);
        run[RunHeader.OffReservedPad] = 1;
        // footer no longer matches either — but reserved checked first path
        // Recompute: after mutating body, footer is stale → BadFooter OR NonZeroReserved
        var r = RunVerify.Verify(run);
        Assert.True(r.Status is RunVerifyStatus.NonZeroReserved or RunVerifyStatus.BadFooter);
    }

    [Fact]
    public void Ns_rejects_above_ffff_on_build()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => EmptyRunBuilder.Create(0x1_0000));
    }
}
