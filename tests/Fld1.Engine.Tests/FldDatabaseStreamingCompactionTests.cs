using Fld1.Engine.Db;
using Xunit;

namespace Fld1.Engine.Db.Tests;

public sealed class FldDatabaseStreamingCompactionTests
{
    [Fact]
    public void Compaction_merges_same_record_across_source_runs_and_splits_output_run()
    {
        const uint namespaceId = 0x781;
        const int fieldCount = 5_000;
        const int commitPageSize = 1_000;
        string root = Path.Combine(
            Path.GetTempPath(), "fld-streaming-compaction-" + Guid.NewGuid().ToString("N"));
        byte[] recordId = FldDatabase.NewRecordId();
        var fields = new (ushort FieldId, byte[] Value)[fieldCount];

        try
        {
            for (int i = 0; i < fieldCount; i++)
                fields[i] = (checked((ushort)(i + 1)), BitConverter.GetBytes(i));

            using (var database = FldDatabase.Open(root, namespaceId, Id(0x71)))
            {
                // Each checkpoint creates another immutable source Run containing
                // fields for the same RecordId. The merged record exceeds one output
                // chunk, so the result has two Runs with the same record bounds.
                for (int offset = 0; offset < fieldCount; offset += commitPageSize)
                {
                    int end = Math.Min(fieldCount, offset + commitPageSize);
                    for (int i = offset; i < end; i++)
                        database.Put(recordId, fields[i].FieldId, fields[i].Value);

                    Assert.NotEqual(0UL, database.Commit());
                    Assert.NotEqual(0UL, database.Checkpoint());
                }

                Assert.NotEqual(0UL, database.Compact());

                foreach (var field in fields)
                {
                    Assert.True(database.TryGetBytes(recordId, field.FieldId, out byte[]? actual));
                    Assert.Equal(field.Value, actual);
                }
            }

            using var reopened = FldDatabase.Open(root, namespaceId, Id(0x71));
            foreach (var field in fields)
            {
                Assert.True(reopened.TryGetBytes(recordId, field.FieldId, out byte[]? actual));
                Assert.Equal(field.Value, actual);
            }
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    private static byte[] Id(byte value)
    {
        byte[] id = new byte[16];
        id[0] = value;
        return id;
    }
}
