using Fld1.Storage.Engine;
using Xunit;

namespace Fld1.Storage.Tests;

public sealed class StorageGenerationPinsTests
{
    [Fact]
    public void Retired_generation_waits_for_all_snapshot_and_cursor_pins()
    {
        var pins = new StorageGenerationPins();
        pins.Register(12);
        using var snapshot = pins.Acquire(12);
        using var cursor = pins.Acquire(12);

        Assert.False(pins.Retire(12));
        Assert.Equal(2, pins.GetPinCount(12));
        Assert.Throws<InvalidOperationException>(() => pins.Acquire(12));
        Assert.False(pins.TryTakeReclaimable(out _));

        snapshot.Dispose();
        Assert.False(pins.TryTakeReclaimable(out _));
        cursor.Dispose();

        Assert.True(pins.TryTakeReclaimable(out ulong reclaimed));
        Assert.Equal(12UL, reclaimed);
        Assert.Throws<InvalidOperationException>(() => pins.Acquire(12));
    }

    [Fact]
    public async Task Concurrent_pins_are_counted_without_underflow()
    {
        var pins = new StorageGenerationPins();
        pins.Register(3);
        await Task.WhenAll(Enumerable.Range(0, 64).Select(_ => Task.Run(() =>
        {
            using var pin = pins.Acquire(3);
            Assert.InRange(pins.GetPinCount(3), 1, 64);
        })));

        Assert.Equal(0, pins.GetPinCount(3));
        Assert.True(pins.Retire(3));
        Assert.True(pins.TryTakeReclaimable(out ulong generation));
        Assert.Equal(3UL, generation);
    }

    [Fact]
    public void Register_rejects_zero_and_duplicate_generation()
    {
        var pins = new StorageGenerationPins();
        Assert.Throws<ArgumentOutOfRangeException>(() => pins.Register(0));
        pins.Register(1);
        Assert.Throws<InvalidOperationException>(() => pins.Register(1));
    }
}
