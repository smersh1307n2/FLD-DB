using Fld1.Engine.Db;
using Xunit;

namespace Fld1.Engine.Db.Tests;

public sealed class FldDatabaseStreamingCompactionTests
{
    [Fact]
    public void Compaction_merges_multiple_source_runs_and_publishes_multiple_bounded_output_runs()
    {
        const uint namespaceId = 0x781;
        const int recordCount = 5_000;
        const int commitPageSize = 1_000;
        string root = Path.Combine(
            Path.GetTempPath(), "fld-streaming-compaction-" + Guid.NewGuid().ToString("N"));
        var records = new (byte[] RecordId, byte[] Value)[recordCount];

        try
        {
            for (int i = 0; i < recordCount; i++)
            {
                records[i] = (FldDatabase.NewRecordId(), BitConverter.GetBytes(i));
            }

            using (var database = FldDatabase.Open(root, namespaceId, Id(0x71)))
            {
                for (int offset = 0; offset < recordCount; offset += commitPageSize)
                {
                    int end = Math.Min(recordCount, offset + commitPageSize);
                    for (int i = offset; i < end; i++)
                        database.Put(records[i].RecordId, 7, records[i].Value);

                    Assert.NotEqual(0UL, database.Commit());
                    Assert.NotEqual(0UL, database.Checkpoint());
                }

                // 5,000 live fields exceed the per-output-Run field target. The return
                // value identifies the final immutable output Run; all chunks are made
                // visible by the same Manifest publication.
                Assert.NotEqual(0UL, database.Compact());

                foreach (var item in records)
                {
                    Assert.True(database.TryGetBytes(item.RecordId, 7, out byte[]? actual));
                    Assert.Equal(item.Value, actual);
                }
            }

            using var reopened = FldDatabase.Open(root, namespaceId, Id(0x71));
            foreach (var item in records)
            {
                Assert.True(reopened.TryGetBytes(item.RecordId, 7, out byte[]? actual));
                Assert.Equal(item.Value, actual);
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
