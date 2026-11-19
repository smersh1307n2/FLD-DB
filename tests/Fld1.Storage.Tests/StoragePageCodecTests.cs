using Fld1.Storage.Engine;
using Xunit;

namespace Fld1.Storage.Tests;

public sealed class StoragePageCodecTests
{
    [Fact]
    public void Page_round_trips_identity_kind_and_payload()
    {
        byte[] payload = Enumerable.Range(0, 4096).Select(value => (byte)value).ToArray();

        byte[] encoded = StoragePageCodec.Encode(7, 19, StoragePageKind.Leaf, payload);
        StoragePageView decoded = StoragePageCodec.Decode(encoded, 7, 19);

        Assert.Equal(StoragePageCodec.PageSize, encoded.Length);
        Assert.Equal(7UL, decoded.Generation);
        Assert.Equal(19UL, decoded.PageId);
        Assert.Equal(StoragePageKind.Leaf, decoded.Kind);
        Assert.Equal(payload, decoded.Payload.ToArray());
    }

    [Fact]
    public void Page_rejects_wrong_size_and_corrupt_payload()
    {
        byte[] encoded = StoragePageCodec.Encode(1, 1, StoragePageKind.Internal, "node"u8);
        Assert.Throws<InvalidDataException>(() => StoragePageCodec.Decode(encoded.AsMemory(0, encoded.Length - 1)));

        encoded[StoragePageCodec.HeaderSize] ^= 0x40;
        Assert.Throws<InvalidDataException>(() => StoragePageCodec.Decode(encoded));
    }

    [Fact]
    public void Page_rejects_manifest_identity_mismatch()
    {
        byte[] encoded = StoragePageCodec.Encode(3, 9, StoragePageKind.Leaf, "leaf"u8);

        Assert.Throws<InvalidDataException>(() => StoragePageCodec.Decode(encoded, 4, 9));
        Assert.Throws<InvalidDataException>(() => StoragePageCodec.Decode(encoded, 3, 10));
    }

    [Fact]
    public void Page_rejects_nonzero_padding_and_reserved_flags()
    {
        byte[] encoded = StoragePageCodec.Encode(1, 1, StoragePageKind.Leaf, "x"u8);
        encoded[^1] = 1;
        Assert.Throws<InvalidDataException>(() => StoragePageCodec.Decode(encoded));

        encoded = StoragePageCodec.Encode(1, 1, StoragePageKind.Leaf, "x"u8);
        encoded[7] = 1;
        Assert.Throws<InvalidDataException>(() => StoragePageCodec.Decode(encoded));
    }

    [Fact]
    public void Page_rejects_unsupported_kind_and_version()
    {
        byte[] encoded = StoragePageCodec.Encode(1, 1, StoragePageKind.Leaf, "x"u8);
        encoded[6] = 0x7F;
        Assert.Throws<InvalidDataException>(() => StoragePageCodec.Decode(encoded));

        encoded = StoragePageCodec.Encode(1, 1, StoragePageKind.Leaf, "x"u8);
        encoded[4] = 2;
        Assert.Throws<InvalidDataException>(() => StoragePageCodec.Decode(encoded));
    }

    [Fact]
    public void Page_encoder_rejects_zero_identity_and_oversized_payload()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            StoragePageCodec.Encode(0, 1, StoragePageKind.Leaf, ReadOnlySpan<byte>.Empty));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            StoragePageCodec.Encode(1, 0, StoragePageKind.Leaf, ReadOnlySpan<byte>.Empty));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            StoragePageCodec.Encode(
                1, 1, StoragePageKind.Leaf,
                new byte[StoragePageCodec.PageSize - StoragePageCodec.HeaderSize + 1]));
    }

    [Fact]
    public void In_place_page_encoder_matches_allocating_encoder()
    {
        byte[] payload = Enumerable.Range(0, 2048).Select(value => (byte)(value * 17)).ToArray();
        byte[] expected = StoragePageCodec.Encode(9, 27, StoragePageKind.Leaf, payload);
        byte[] reusable = new byte[StoragePageCodec.PageSize];

        payload.CopyTo(reusable.AsSpan(StoragePageCodec.HeaderSize));
        StoragePageCodec.EncodeInPlace(
            reusable, 9, 27, StoragePageKind.Leaf, payload.Length);

        Assert.Equal(expected, reusable);
        StoragePageView decoded = StoragePageCodec.Decode(reusable, 9, 27);
        Assert.Equal(payload, decoded.Payload.ToArray());
    }
}
