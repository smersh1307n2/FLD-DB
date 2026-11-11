using Fld1.Storage.Ipc;
using Fld1.Engine.Outbox;
using Xunit;
namespace Fld1.Engine.Db.Tests;

public sealed class FldDatabaseIpcAutoSequenceTests
{
    [Fact]
    public async Task Auto_sequence_append_retries_same_change_id_without_client_side_sequence_state()
    {
        string root=Path.Combine(Path.GetTempPath(),"fld-auto-seq-ipc-"+Guid.NewGuid().ToString("N"));
        const uint ns=0x532;byte[] author=Id(0x41);Guid owner=Guid.NewGuid(),clientId=Guid.NewGuid();
        byte[] record=FldDatabase.NewRecordId(),change=Id(0x75),value="durable-auto-sequence"u8.ToArray();
        using var db=FldDatabase.Open(root,ns,author);using var dispatcher=new FldDatabaseIpcDispatcher(db);
        using var shutdown=new CancellationTokenSource();var server=new StorageIpcServer(StorageIpcEndpoint.ComputePipeName(root,0),dispatcher.HandleAsync);
        Task task=server.RunAsync(shutdown.Token);
        try{
            await server.Ready.WaitAsync(TimeSpan.FromSeconds(10));StorageIpcEndpoint.Publish(root,StorageIpcEndpoint.Create(root,0,owner,Environment.ProcessId));
            await using var client=await FldDatabaseIpcClient.ConnectToDatabaseAsync(root,clientId,TimeSpan.FromSeconds(10));
            var mutation=new FldLocalProviderMutation(record,6,value,change,8,12345,2,4);
            Guid operationId=new(change,bigEndian:true);
            var first=await client.AppendLocalProviderBatchAutoSequenceAsync(operationId,[mutation]);
            Assert.False(first.IsDuplicate);Assert.Equal(1,first.MutationCount);
            var retry=await client.AppendLocalProviderBatchAutoSequenceAsync(operationId,[mutation]);
            Assert.True(retry.IsDuplicate);Assert.Equal(first.CommitSeq,retry.CommitSeq);
            Assert.Equal(1ul,db.GetOutboxHighWatermark());
            var row=Assert.Single(db.ReadOutboxBatch(0,10));Assert.Equal(change,row.ChangeId);
            var secondMutation=new FldLocalProviderMutation(record,7,"second"u8.ToArray(),Id(0x76),8,12346,0,4);
            var second=await client.AppendLocalProviderBatchAutoSequenceAsync(Guid.NewGuid(),[secondMutation]);
            Assert.False(second.IsDuplicate);Assert.Equal(2ul,db.GetOutboxHighWatermark());
        }finally{shutdown.Cancel();try{await task.WaitAsync(TimeSpan.FromSeconds(10));}catch{}StorageIpcEndpoint.DeleteIfOwned(root,0,owner);dispatcher.Dispose();db.Dispose();try{Directory.Delete(root,true);}catch{}}
    }

    [Fact]
    public void Provider_outbox_ack_retires_receipt_only_after_the_entire_wal_commit_is_covered()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld-provider-ack-" + Guid.NewGuid().ToString("N"));
        const uint ns = 0x533;
        byte[] author = Id(0x41);
        Guid clientGuid = Guid.NewGuid();
        byte[] clientId = new byte[16];
        clientGuid.TryWriteBytes(clientId, bigEndian: true, out _);
        byte[] record = FldDatabase.NewRecordId();
        byte[] firstChange = Id(0x75);
        byte[] secondChange = Id(0x76);
        var first = new FldLocalProviderMutation(record, 6, "first"u8.ToArray(), firstChange, 8, 12345, 2, 4);
        var second = new FldLocalProviderMutation(record, 7, "second"u8.ToArray(), secondChange, 8, 12346, 0, 4);
        Guid operationGuid = new(firstChange, bigEndian: true);
        byte[] operationId = new byte[16];
        operationGuid.TryWriteBytes(operationId, bigEndian: true, out _);
        byte[] payload = FldDatabaseIpcLocalProviderProtocol.EncodeAutoSequenceRequest([first, second]);
        byte[] fingerprint = FldDatabaseIpcProtocol.ComputeFingerprint(
            FldDatabaseIpcOperationCodes.AppendLocalProviderBatchAutoSequence, payload);

        try
        {
            using var db = FldDatabase.Open(root, ns, author);
            OutboxLease lease = db.AcquireOutboxLease(clientId);
            try
            {
                FldLocalOperationCommitResult committed = db.ApplyLocalProviderOperationWithAutoSequence(
                    [first, second], clientId, operationId, fingerprint);
                Assert.False(committed.IsDuplicate);
                Assert.Equal(2ul, db.GetOutboxHighWatermark());

                db.AcknowledgeProviderOutbox(1, lease);
                Assert.True(db.TryGetLocalOperationReceipt(
                    clientId, operationId, 1, fingerprint, out FldLocalOperationCommitResult? pending));
                Assert.True(pending!.IsDuplicate);
                Assert.Equal(1ul, db.GetOutboxWatermark());

                db.AcknowledgeProviderOutbox(2, lease);
                Assert.Equal(2ul, db.GetOutboxWatermark());
                Assert.Throws<InvalidOperationException>(() =>
                    db.TryGetLocalOperationReceipt(clientId, operationId, 1, fingerprint, out _));
            }
            finally
            {
                db.ReleaseOutboxLease(lease);
            }
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Auto_sequence_codec_requires_zero_sequence_and_roundtrips_mutations()
    {
        var m=new FldLocalProviderMutation(FldDatabase.NewRecordId(),2,"x"u8.ToArray(),Id(0x33),4,123,0,1);
        byte[] p=FldDatabaseIpcLocalProviderProtocol.EncodeAutoSequenceRequest([m]);
        Assert.Equal(0ul,System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(p.AsSpan(1)));
        Assert.Equal(m.ChangeId,Assert.Single(FldDatabaseIpcLocalProviderProtocol.DecodeAutoSequenceRequest(p)).ChangeId);
        byte[] wrong=FldDatabaseIpcLocalProviderProtocol.EncodeRequest(1,[m]);
        Assert.Throws<InvalidDataException>(()=>FldDatabaseIpcLocalProviderProtocol.DecodeAutoSequenceRequest(wrong));
    }
    private static byte[] Id(byte value){byte[] b=new byte[16];b[0]=value;return b;}
}
