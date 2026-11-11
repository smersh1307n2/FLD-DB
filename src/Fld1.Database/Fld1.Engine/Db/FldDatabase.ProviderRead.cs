using Fld1.Engine.Storage;
namespace Fld1.Engine.Db;

public sealed record FldProviderCurrentMetadata(uint ValueLength,ulong HlcWall,uint HlcLogical,byte[] AuthorNodeId,ushort KeyEpoch,bool IsTombstone,byte[] ChangeId,uint OriginAuthorEpoch,ulong OriginSequence);
public enum FldProviderCurrentReadStatus : byte { Missing=0, Changed=1, Data=2 }
public sealed record FldProviderCurrentValueChunk(FldProviderCurrentReadStatus Status,uint ValueLength,uint ValueOffset,byte[] ValueChunk);

public sealed partial class FldDatabase
{
    /// <summary>Addressed metadata lookup through the existing indexed LWW resolver.</summary>
    public bool TryReadProviderCurrentMetadata(ReadOnlySpan<byte> recordId,ushort fieldId,out FldProviderCurrentMetadata? metadata)
    {
        metadata=null;
        if(!TryResolve(recordId,fieldId,out FieldResolver.Resolved r)||!r.Found)return false;
        byte[] author=r.AuthorNodeId is {Length:16}?r.AuthorNodeId.ToArray():new byte[16];
        byte[] change=r.ChangeId is {Length:16}?r.ChangeId.ToArray():new byte[16];
        uint length=r.IsTombstone?0u:checked((uint)(r.Value?.Length??0));
        metadata=new(length,r.Wall,r.Logical,author,r.KeyEpoch,r.IsTombstone,change,r.OriginAuthorEpoch,r.OriginSequence);return true;
    }

    /// <summary>Reads a bounded value slice only if the current ChangeId still matches.</summary>
    public FldProviderCurrentValueChunk ReadProviderCurrentValueChunk(ReadOnlySpan<byte> recordId,ushort fieldId,ReadOnlySpan<byte> expectedChangeId,uint valueOffset,uint maxChunkBytes)
    {
        if(recordId.Length!=16)throw new ArgumentException("RecordId must be 16 bytes.",nameof(recordId));
        if(expectedChangeId.Length!=16)throw new ArgumentException("Expected ChangeId must be 16 bytes.",nameof(expectedChangeId));
        if(maxChunkBytes is 0 or > FldDatabaseIpcProtocol.MaxCurrentValueChunkBytes)throw new ArgumentOutOfRangeException(nameof(maxChunkBytes));
        if(!TryResolve(recordId,fieldId,out FieldResolver.Resolved r)||!r.Found)return new(FldProviderCurrentReadStatus.Missing,0,0,Array.Empty<byte>());
        if(r.ChangeId is not {Length:16}||!r.ChangeId.AsSpan().SequenceEqual(expectedChangeId))return new(FldProviderCurrentReadStatus.Changed,0,0,Array.Empty<byte>());
        byte[] value=r.IsTombstone?Array.Empty<byte>():r.Value??Array.Empty<byte>();
        if(valueOffset>value.Length)throw new ArgumentOutOfRangeException(nameof(valueOffset));
        int count=checked((int)Math.Min(maxChunkBytes,(uint)(value.Length-valueOffset)));
        return new(FldProviderCurrentReadStatus.Data,checked((uint)value.Length),valueOffset,value.AsSpan(checked((int)valueOffset),count).ToArray());
    }
}
