using Fld1.Core.Uuid;
using Xunit;

namespace Fld1.Core.Tests;

public class Uuid7Tests
{
    [Fact]
    public void NewUuid7_is_valid()
    {
        Span<byte> u = stackalloc byte[16];
        Uuid7.NewUuid7(u);
        Assert.True(Uuid7.IsValidV7(u));
        Assert.Equal(0x7, (u[6] >> 4) & 0xF);
        Assert.Equal(0b10, (u[8] >> 6) & 0b11);
    }

    [Fact]
    public void ProjectK_monotonic_with_time()
    {
        Span<byte> u1 = stackalloc byte[16];
        Span<byte> u2 = stackalloc byte[16];
        Uuid7.NewUuid7(u1);
        Thread.Sleep(2);
        Uuid7.NewUuid7(u2);
        ulong k1 = Uuid7.ProjectK(u1);
        ulong k2 = Uuid7.ProjectK(u2);
        Assert.True(k2 >= k1);
    }

    [Fact]
    public void ShortKey_last4()
    {
        Span<byte> u = stackalloc byte[16];
        for (int i = 0; i < 16; i++) u[i] = (byte)i;
        // bytes 12..15 = 0C 0D 0E 0F → BE uint
        uint sk = Uuid7.ShortKey(u);
        Assert.Equal(0x0C0D0E0Fu, sk);
    }

    [Fact]
    public void Invalid_version_rejected()
    {
        Span<byte> u = stackalloc byte[16];
        Uuid7.NewUuid7(u);
        u[6] = (byte)((u[6] & 0x0F) | 0x40); // version 4
        Assert.False(Uuid7.IsValidV7(u));
    }
}
