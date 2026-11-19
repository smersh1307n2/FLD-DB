using Fld1.Storage.Engine;
using Fld1.Storage.Ipc;
using Xunit;

namespace Fld1.Storage.Tests;

public sealed class StorageIpcOperationLedgerTests
{
    [Fact]
    public void Operation_result_and_storage_mutation_commit_atomically_and_replay_after_reopen()
    {
        string directory = Path.Combine(
            Path.GetTempPath(), "fld-storage-ipc-ledger-" + Guid.NewGuid().ToString("N"));
        const ulong partition = 0x1D01;
        Guid clientId = Guid.Parse("1d010000-0000-0000-0000-000000000001");
        Guid operationId = Guid.Parse("1d010000-0000-0000-0000-000000000002");
        byte[] dataKey = "record/one/field/7"u8.ToArray();
        byte[] dataValue = "committed-value"u8.ToArray();
        byte[] requestPayload = "put:record-one-field-seven"u8.ToArray();
        byte[] fingerprint = StorageIpcOperationLedger.ComputeFingerprint(7, requestPayload);
        byte[] result = "accepted:commit-1"u8.ToArray();

        try
        {
            using (var database = StorageDatabase.Open(directory, partition))
            {
                var ledger = new StorageIpcOperationLedger(database);
                using var batch = database.BeginWriteBatch();
                batch.Put(dataKey, dataValue);
                Assert.True(ledger.TryStageResult(
                    batch, clientId, operationId, fingerprint, result, out byte[]? existing));
                Assert.Null(existing);
                batch.Commit();

                Assert.True(database.TryGet(dataKey, out byte[]? stored));
                Assert.Equal(dataValue, stored);
                Assert.True(ledger.TryGetResult(clientId, operationId, fingerprint, out byte[]? firstResult));
                Assert.Equal(result, firstResult);
            }

            using (var reopened = StorageDatabase.Open(directory, partition))
            {
                var ledger = new StorageIpcOperationLedger(reopened);
                Assert.True(reopened.TryGet(dataKey, out byte[]? stored));
                Assert.Equal(dataValue, stored);
                Assert.True(ledger.TryGetResult(clientId, operationId, fingerprint, out byte[]? recovered));
                Assert.Equal(result, recovered);

                using var duplicateBatch = reopened.BeginWriteBatch();
                Assert.False(ledger.TryStageResult(
                    duplicateBatch, clientId, operationId, fingerprint,
                    "must-not-replace-the-original-result"u8, out byte[]? prior));
                Assert.Equal(result, prior);
                // The caller abandons duplicateBatch; it must not commit another mutation.
            }
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Operation_result_and_data_mutation_recover_together_after_flush_before_publish_crash()
    {
        string directory = Path.Combine(
            Path.GetTempPath(), "fld-storage-ipc-ledger-crash-" + Guid.NewGuid().ToString("N"));
        const ulong partition = 0x1D02;
        Guid clientId = Guid.Parse("1d020000-0000-0000-0000-000000000001");
        Guid operationId = Guid.Parse("1d020000-0000-0000-0000-000000000002");
        byte[] dataKey = "record/crash/field/2"u8.ToArray();
        byte[] dataValue = "durable-before-publish"u8.ToArray();
        byte[] fingerprint = StorageIpcOperationLedger.ComputeFingerprint(2, "put-crash-case"u8);
        byte[] result = "commit-result"u8.ToArray();

        try
        {
            using (var database = StorageDatabase.OpenForTesting(
                directory,
                partition,
                point =>
                {
                    if (point == StorageFaultPoint.AfterWalFlushBeforePublish)
                        throw new IOException("simulated process loss after durable WAL flush");
                }))
            {
                var ledger = new StorageIpcOperationLedger(database);
                using var batch = database.BeginWriteBatch();
                batch.Put(dataKey, dataValue);
                Assert.True(ledger.TryStageResult(
                    batch, clientId, operationId, fingerprint, result, out _));
                Assert.Throws<IOException>(() => batch.Commit());
            }

            using (var recovered = StorageDatabase.Open(directory, partition))
            {
                var ledger = new StorageIpcOperationLedger(recovered);
                Assert.True(recovered.TryGet(dataKey, out byte[]? stored));
                Assert.Equal(dataValue, stored);
                Assert.True(ledger.TryGetResult(clientId, operationId, fingerprint, out byte[]? recoveredResult));
                Assert.Equal(result, recoveredResult);
            }
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Concurrent_batches_for_the_same_operation_identity_cannot_both_commit()
    {
        string directory = Path.Combine(
            Path.GetTempPath(), "fld-storage-ipc-ledger-race-" + Guid.NewGuid().ToString("N"));
        const ulong partition = 0x1D05;
        Guid clientId = Guid.NewGuid();
        Guid operationId = Guid.NewGuid();
        byte[] fingerprint = StorageIpcOperationLedger.ComputeFingerprint(5, "same-operation"u8);

        try
        {
            using var database = StorageDatabase.Open(directory, partition);
            var ledger = new StorageIpcOperationLedger(database);
            using var first = database.BeginWriteBatch();
            using var second = database.BeginWriteBatch();

            first.Put("first-effect"u8, "committed"u8);
            second.Put("second-effect"u8, "must-not-commit"u8);
            Assert.True(ledger.TryStageResult(
                first, clientId, operationId, fingerprint, "durable-result"u8, out _));
            Assert.True(ledger.TryStageResult(
                second, clientId, operationId, fingerprint, "durable-result"u8, out _));

            first.Commit();
            Assert.Throws<StorageConflictException>(() => second.Commit());

            Assert.True(database.TryGet("first-effect"u8, out byte[]? firstValue));
            Assert.Equal("committed"u8.ToArray(), firstValue);
            Assert.False(database.TryGet("second-effect"u8, out _));
            Assert.True(ledger.TryGetResult(clientId, operationId, fingerprint, out byte[]? replayed));
            Assert.Equal("durable-result"u8.ToArray(), replayed);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Operation_ledger_rejects_a_write_batch_from_a_different_storage_database()
    {
        string firstDirectory = Path.Combine(
            Path.GetTempPath(), "fld-storage-ipc-ledger-owner-" + Guid.NewGuid().ToString("N"));
        string secondDirectory = Path.Combine(
            Path.GetTempPath(), "fld-storage-ipc-ledger-foreign-" + Guid.NewGuid().ToString("N"));

        try
        {
            using var first = StorageDatabase.Open(firstDirectory, 0x1D04);
            using var second = StorageDatabase.Open(secondDirectory, 0x1D04);
            var ledger = new StorageIpcOperationLedger(first);
            using var foreignBatch = second.BeginWriteBatch();

            Assert.Throws<ArgumentException>(() => ledger.TryStageResult(
                foreignBatch,
                Guid.NewGuid(),
                Guid.NewGuid(),
                StorageIpcOperationLedger.ComputeFingerprint(1, "request"u8),
                "result"u8,
                out _));
        }
        finally
        {
            try { Directory.Delete(firstDirectory, recursive: true); } catch { }
            try { Directory.Delete(secondDirectory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Reusing_operation_identity_with_different_fingerprint_fails_closed()
    {
        string directory = Path.Combine(
            Path.GetTempPath(), "fld-storage-ipc-ledger-conflict-" + Guid.NewGuid().ToString("N"));
        const ulong partition = 0x1D03;
        Guid clientId = Guid.NewGuid();
        Guid operationId = Guid.NewGuid();
        byte[] firstFingerprint = StorageIpcOperationLedger.ComputeFingerprint(3, "first-payload"u8);
        byte[] conflictingFingerprint = StorageIpcOperationLedger.ComputeFingerprint(3, "different-payload"u8);

        try
        {
            using var database = StorageDatabase.Open(directory, partition);
            var ledger = new StorageIpcOperationLedger(database);
            using (var batch = database.BeginWriteBatch())
            {
                batch.Put("data"u8, "value"u8);
                Assert.True(ledger.TryStageResult(
                    batch, clientId, operationId, firstFingerprint, "first-result"u8, out _));
                batch.Commit();
            }

            Assert.Throws<InvalidOperationException>(() =>
                ledger.TryGetResult(clientId, operationId, conflictingFingerprint, out _));

            using var duplicateBatch = database.BeginWriteBatch();
            Assert.Throws<InvalidOperationException>(() =>
                ledger.TryStageResult(
                    duplicateBatch, clientId, operationId, conflictingFingerprint,
                    "conflicting-result"u8, out _));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }
}
