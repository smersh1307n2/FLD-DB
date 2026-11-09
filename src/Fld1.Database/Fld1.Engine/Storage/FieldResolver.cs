using Fld1.Core.Util;
using Fld1.Engine.Storage;
using Fld1.Run.Reader;

namespace Fld1.Engine.Storage;

/// <summary>LWW winner across overlay + runs. Spec: wall, logical, authorNodeId (NodeId order).</summary>
public static class FieldResolver
{
    public readonly struct Resolved
    {
        public readonly bool Found;
        public readonly bool IsTombstone;
        public readonly ushort KeyEpoch;
        public readonly ulong Wall;
        public readonly uint Logical;
        public readonly byte[]? AuthorNodeId;
        public readonly byte[]? Value;
        public readonly byte[]? ChangeId;
        public readonly uint OriginAuthorEpoch;
        public readonly ulong OriginSequence;

        public Resolved(
            bool found, bool tomb, ushort keyEpoch, ulong wall, uint logical,
            byte[]? author, byte[]? value, byte[]? changeId = null,
            uint originAuthorEpoch = 0, ulong originSequence = 0)
        {
            Found = found; IsTombstone = tomb; KeyEpoch = keyEpoch;
            Wall = wall; Logical = logical; AuthorNodeId = author; Value = value;
            ChangeId = changeId; OriginAuthorEpoch = originAuthorEpoch;
            OriginSequence = originSequence;
        }

        public static Resolved Miss => default;
    }

    public static int CompareHlc(
        ulong wallA, uint logA, ReadOnlySpan<byte> authorA,
        ulong wallB, uint logB, ReadOnlySpan<byte> authorB)
    {
        int c = wallA.CompareTo(wallB);
        if (c != 0) return c;
        c = logA.CompareTo(logB);
        if (c != 0) return c;
        if (authorA.Length == 16 && authorB.Length == 16)
            return ByteUtil.CompareAuthorNodeId(authorA, authorB);
        return authorA.Length.CompareTo(authorB.Length);
    }

    public static bool IsNewer(
        ulong wallA, uint logA, ReadOnlySpan<byte> authorA,
        ulong wallB, uint logB, ReadOnlySpan<byte> authorB)
        => CompareHlc(wallA, logA, authorA, wallB, logB, authorB) > 0;
}
