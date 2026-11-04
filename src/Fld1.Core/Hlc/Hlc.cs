using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Fld1.Core.Util;

namespace Fld1.Core.Hlc;

/// <summary>
/// Hybrid Logical Clock: wall (microseconds UTC) + logical (u32) + authorNodeId (16B).
/// Spec: [HLC-UNIT] wall = microseconds; no ms↔µs implicit conversion.
/// [HLC-OVERFLOW] logical ≥ 2³²−1 → LOGICAL_OVERFLOW.
/// [HLC-VALID] wall > 0 in stored fields.
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 1)]
public readonly struct HlcStamp : IComparable<HlcStamp>, IEquatable<HlcStamp>
{
    public readonly ulong Wall;       // microseconds UTC
    public readonly uint Logical;

    public HlcStamp(ulong wall, uint logical)
    {
        Wall = wall;
        Logical = logical;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int CompareTo(HlcStamp other)
    {
        int c = Wall.CompareTo(other.Wall);
        if (c != 0) return c;
        return Logical.CompareTo(other.Logical);
    }

    public bool Equals(HlcStamp other) => Wall == other.Wall && Logical == other.Logical;
    public override bool Equals(object? obj) => obj is HlcStamp h && Equals(h);
    public override int GetHashCode() => HashCode.Combine(Wall, Logical);

    public static bool operator <(HlcStamp a, HlcStamp b) => a.CompareTo(b) < 0;
    public static bool operator >(HlcStamp a, HlcStamp b) => a.CompareTo(b) > 0;
    public static bool operator <=(HlcStamp a, HlcStamp b) => a.CompareTo(b) <= 0;
    public static bool operator >=(HlcStamp a, HlcStamp b) => a.CompareTo(b) >= 0;
    public static bool operator ==(HlcStamp a, HlcStamp b) => a.Equals(b);
    public static bool operator !=(HlcStamp a, HlcStamp b) => !a.Equals(b);
}

/// <summary>
/// Full HLC with author for LWW tie-break: (Wall, Logical, AuthorNodeId).
/// </summary>
public readonly struct HlcFull : IComparable<HlcFull>, IEquatable<HlcFull>
{
    public readonly HlcStamp Stamp;
    public readonly ReadOnlyMemory<byte> AuthorNodeId; // 16B

    public HlcFull(HlcStamp stamp, ReadOnlyMemory<byte> authorNodeId)
    {
        if (authorNodeId.Length != 16)
            throw new ArgumentException("AuthorNodeId must be 16 bytes");
        Stamp = stamp;
        AuthorNodeId = authorNodeId;
    }

    public HlcFull(ulong wall, uint logical, ReadOnlyMemory<byte> authorNodeId)
        : this(new HlcStamp(wall, logical), authorNodeId) { }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int CompareTo(HlcFull other)
    {
        int c = Stamp.CompareTo(other.Stamp);
        if (c != 0) return c;
        return ByteUtil.CompareAuthorNodeId(AuthorNodeId.Span, other.AuthorNodeId.Span);
    }

    public bool Equals(HlcFull other)
        => Stamp.Equals(other.Stamp)
           && ByteUtil.Equals16(AuthorNodeId.Span, other.AuthorNodeId.Span);

    public override bool Equals(object? obj) => obj is HlcFull h && Equals(h);
    public override int GetHashCode()
    {
        var s = AuthorNodeId.Span;
        ulong a = System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(s);
        ulong b = System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(s.Slice(8));
        return HashCode.Combine(Stamp.Wall, Stamp.Logical, a, b);
    }
}

/// <summary>
/// HLC clock generator for a local node.
/// </summary>
public sealed class HlcClock
{
    private readonly byte[] _author = new byte[16];
    private ulong _lastWall;
    private uint _logical;
    private readonly object _lock = new();

    public HlcClock(ReadOnlySpan<byte> authorNodeId)
    {
        if (authorNodeId.Length != 16)
            throw new ArgumentException("AuthorNodeId must be 16 bytes");
        authorNodeId.CopyTo(_author);
    }

    public ReadOnlySpan<byte> AuthorNodeId => _author;

    /// <summary>
    /// Produce next HLC. wall in microseconds.
    /// </summary>
    public HlcFull Now()
    {
        lock (_lock)
        {
            ulong now = (ulong)(DateTimeOffset.UtcNow.Ticks / 10); // 100ns → µs
            if (now > _lastWall)
            {
                _lastWall = now;
                _logical = 0;
            }
            else
            {
                // same or behind physical time → tick logical
                if (_logical == uint.MaxValue)
                    throw new InvalidOperationException("LOGICAL_OVERFLOW");
                _logical++;
                if (now < _lastWall)
                {
                    // keep wall
                }
                else
                {
                    _lastWall = now;
                }
            }
            return new HlcFull(_lastWall, _logical, _author);
        }
    }

    /// <summary>
    /// Observe a remote HLC and advance local clock past it.
    /// After Observe(max logical), the next Now() on the same wall will overflow.
    /// </summary>
    public void Observe(HlcStamp remote)
    {
        lock (_lock)
        {
            if (remote.Wall > _lastWall)
            {
                _lastWall = remote.Wall;
                _logical = remote.Logical;
            }
            else if (remote.Wall == _lastWall)
            {
                if (remote.Logical > _logical)
                    _logical = remote.Logical;
                // if equal, next Now() will try to increment
            }
            // older remote ignored for wall
        }
    }
}
