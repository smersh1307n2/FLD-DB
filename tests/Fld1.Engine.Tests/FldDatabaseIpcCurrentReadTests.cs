using Fld1.Storage.Ipc;
using Xunit;
namespace Fld1.Engine.Db.Tests;

public sealed class FldDatabaseIpcCurrentReadTests
{
    [Fact]
    public async Task Current_reads_are_addressed_and_value_chunks_are_fenced_by_change_id()
    {
        string root=Path.Combine(Path.GetTempPath(),"fld-current-ipc-"+Guid.NewGuid().ToString("N"));
        const uint ns=0x531;byte[] author=Id(0x41);Guid owner=Guid.NewGuid(),clientId=Guid.NewGuid();
        byte[] record=FldDatabase.NewRecordId(),change1=Id(0x71),value1="first-current-value"u8.ToArray();
        using var db=FldDatabase.Open(root,ns,author);using var dispatcher=new FldDatabaseIpcDispatcher(db);
        using var shutdown=new CancellationTokenSource();var server=new StorageIpcServer(StorageIpcEndpoint.ComputePipeName(root,0),dispatcher.HandleAsync);
        Task serverTask=server.RunAsync(shutdown.Token);
        try{
            await server.Ready.WaitAsync(TimeSpan.FromSeconds(10));StorageIpcEndpoint.Publish(root,StorageIpcEndpoint.Create(root,0,owner,Environment.ProcessId));
            await using var c=await FldDatabaseIpcClient.ConnectToDatabaseAsync(root,clientId,TimeSpan.FromSeconds(10));
            var first=new FldLocalProviderMutation(record,7,value1,change1,9,1234,5,3);
            await c.AppendLocalProviderBatchAsync(Guid.NewGuid(),1,[first]);
            var m=await c.ReadCurrentMetadataAsync(record,7);Assert.NotNull(m);Assert.Equal((uint)value1.Length,m.ValueLength);
            Assert.Equal(1234ul,m.HlcWall);Assert.Equal(5u,m.HlcLogical);Assert.Equal(author,m.AuthorNodeId);Assert.Equal((ushort)3,m.KeyEpoch);
            Assert.False(m.IsTombstone);Assert.Equal(change1,m.ChangeId);Assert.Equal(9u,m.OriginAuthorEpoch);Assert.Equal(0ul,m.OriginSequence);
            var part=await c.ReadCurrentValueChunkAsync(record,7,change1,0,5);Assert.Equal(FldDatabaseIpcCurrentReadStatus.Data,part.Status);Assert.Equal("first"u8.ToArray(),part.ValueChunk);
            byte[] change2=Id(0x72),value2="replacement-value"u8.ToArray();
            await c.AppendLocalProviderBatchAsync(Guid.NewGuid(),2,[new FldLocalProviderMutation(record,7,value2,change2,9,1235,0,3)]);
            var stale=await c.ReadCurrentValueChunkAsync(record,7,change1,5,5);Assert.Equal(FldDatabaseIpcCurrentReadStatus.Changed,stale.Status);Assert.Empty(stale.ValueChunk);
            var latest=await c.ReadCurrentMetadataAsync(record,7);Assert.NotNull(latest);Assert.Equal(change2,latest.ChangeId);Assert.Equal((uint)value2.Length,latest.ValueLength);
            var all=await c.ReadCurrentValueChunkAsync(record,7,change2,0,64);Assert.Equal(FldDatabaseIpcCurrentReadStatus.Data,all.Status);Assert.True(all.IsComplete);Assert.Equal(value2,all.ValueChunk);
            Assert.Null(await c.ReadCurrentMetadataAsync(FldDatabase.NewRecordId(),7));
        }finally{shutdown.Cancel();try{await serverTask.WaitAsync(TimeSpan.FromSeconds(10));}catch{}StorageIpcEndpoint.DeleteIfOwned(root,0,owner);dispatcher.Dispose();db.Dispose();try{Directory.Delete(root,true);}catch{}}
    }
    [Fact] public void Current_read_codecs_reject_invalid_lengths_and_chunk_sizes()
    {
        Assert.Throws<ArgumentException>(()=>FldDatabaseIpcProtocol.EncodeReadCurrentMetadataPayload(new byte[15],1));
        Assert.Throws<ArgumentOutOfRangeException>(()=>FldDatabaseIpcProtocol.EncodeReadCurrentValueChunkPayload(new byte[16],1,new byte[16],0,0));
        Assert.Throws<InvalidDataException>(()=>FldDatabaseIpcProtocol.DecodeReadCurrentValueChunkPayload(new byte[41]));
        Assert.Null(FldDatabaseIpcProtocol.DecodeCurrentMetadataResponse(new byte[64]));
    }
    private static byte[] Id(byte value){byte[] b=new byte[16];b[0]=value;return b;}
}
