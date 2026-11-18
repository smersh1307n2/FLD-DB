using Fld1.Engine.Db;
using Xunit;

namespace Fld1.Engine.Tests;

public sealed class LocalAuthorTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "fld-author-" + Guid.NewGuid().ToString("N"));

    public LocalAuthorTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try { Directory.Delete(_dir, true); } catch { }
    }

    private static byte[] Author(byte value)
    {
        var author = new byte[16];
        author[15] = value;
        return author;
    }

    [Fact]
    public void Generated_local_author_is_stable_across_reopen()
    {
        byte[] first;
        using (var db = FldDatabase.Open(_dir))
        {
            first = db.LocalAuthor.ToArray();
            db.Put(FldDatabase.NewRecordId(), 1, "value"u8);
            db.Commit();
        }

        using var reopened = FldDatabase.Open(_dir);
        Assert.Equal(first, reopened.LocalAuthor.ToArray());
    }

    [Fact]
    public void Explicit_local_author_must_match_persisted_identity()
    {
        using (FldDatabase.Open(_dir, localAuthor: Author(1))) { }

        Assert.Throws<InvalidOperationException>(
            () => FldDatabase.Open(_dir, localAuthor: Author(2)));
    }

    [Fact]
    public void Legacy_nonempty_database_requires_explicit_local_author()
    {
        var recordId = FldDatabase.NewRecordId();
        using (var db = FldDatabase.Open(_dir, localAuthor: Author(7)))
        {
            db.Put(recordId, 1, "legacy"u8);
            db.Commit();
        }

        File.Delete(Path.Combine(_dir, "local-author.flda"));

        Assert.Throws<InvalidOperationException>(() => FldDatabase.Open(_dir));

        using var migrated = FldDatabase.Open(_dir, localAuthor: Author(7));
        Assert.Equal(Author(7), migrated.LocalAuthor.ToArray());
        Assert.True(migrated.TryGetBytes(recordId, 1, out var value));
        Assert.Equal("legacy"u8.ToArray(), value);
    }
}
