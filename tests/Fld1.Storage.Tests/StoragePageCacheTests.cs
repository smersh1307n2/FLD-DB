using Fld1.Storage.Engine;
using Xunit;

namespace Fld1.Storage.Tests;

public sealed class StoragePageCacheTests
{

    [Fact]
    public void Cache_lease_decodes_and_retains_validated_page_view_for_resident_entry()
    {
        byte[] encoded = StoragePageCodec.Encode(7, 3, StoragePageKind.Leaf, new byte[] { 1, 2, 3 });
        var cache = new StoragePageCache(StoragePageCodec.PageSize);
        using var lease = cache.Acquire(new StoragePageKey(7, 3), () => encoded);

        StoragePageView first = lease.Decode(7, 3);
        StoragePageView second = lease.Decode(7, 3);

        Assert.Equal(StoragePageKind.Leaf, first.Kind);
        Assert.Equal(first.Generation, second.Generation);
        Assert.Equal(first.PageId, second.PageId);
        Assert.Equal(new byte[] { 1, 2, 3 }, second.Payload.ToArray());
        Assert.Throws<InvalidDataException>(() => lease.Decode(7, 4));
    }

    [Fact]
    public void Cache_evicts_least_recently_used_unpinned_page_within_budget()
    {
        var cache = new StoragePageCache(StoragePageCodec.PageSize * 2L);
        var first = cache.Acquire(new(1, 1), () => Page(1));
        first.Dispose();
        var second = cache.Acquire(new(1, 2), () => Page(2));
        second.Dispose();

        using var touchFirst = cache.Acquire(new(1, 1), () => throw new Xunit.Sdk.XunitException("Expected cache hit."));
        using var third = cache.Acquire(new(1, 3), () => Page(3));

        Assert.Equal(2, cache.Count);
        Assert.Equal(StoragePageCodec.PageSize * 2L, cache.ResidentBytes);
        Assert.Equal(1, cache.EvictionCount);
        Assert.Throws<Xunit.Sdk.XunitException>(() =>
        {
            using var unexpected = cache.Acquire(new(1, 2), () => throw new Xunit.Sdk.XunitException("Evicted page should reload."));
        });
    }

    [Fact]
    public void Cache_never_exceeds_budget_when_all_pages_are_pinned()
    {
        var cache = new StoragePageCache(StoragePageCodec.PageSize);
        using var pinned = cache.Acquire(new(1, 1), () => Page(1));

        Assert.Throws<InvalidOperationException>(() =>
            cache.Acquire(new(1, 2), () => Page(2)));
        Assert.Equal(StoragePageCodec.PageSize, cache.ResidentBytes);

        pinned.Dispose();
        using var replacement = cache.Acquire(new(1, 2), () => Page(2));
        Assert.Equal(StoragePageCodec.PageSize, cache.ResidentBytes);
    }

    [Fact]
    public void Cache_key_includes_generation_and_lease_prevents_eviction()
    {
        var cache = new StoragePageCache(StoragePageCodec.PageSize * 2L);
        using var oldPage = cache.Acquire(new(1, 7), () => Page(1));
        using var newPage = cache.Acquire(new(2, 7), () => Page(2));

        Assert.NotEqual(oldPage.Page.Span[0], newPage.Page.Span[0]);
        cache.EvictGeneration(1);
        Assert.Equal(2, cache.Count);
        oldPage.Dispose();
        cache.EvictGeneration(1);
        Assert.Equal(1, cache.Count);
        Assert.Throws<ObjectDisposedException>(() => _ = oldPage.Page);
    }

    private static byte[] Page(byte marker)
    {
        byte[] page = new byte[StoragePageCodec.PageSize];
        page[0] = marker;
        return page;
    }
}
