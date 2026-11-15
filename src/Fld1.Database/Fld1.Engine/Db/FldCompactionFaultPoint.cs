namespace Fld1.Engine.Db;

// Internal deterministic interruption points for provider tombstone-compaction recovery tests.
internal enum FldCompactionFaultPoint
{
    AfterReplacementRunDurable,
    AfterManifestRenameBeforeDirectoryFlush,
    AfterManifestAndOutboxPublished,
    AfterOldRunDeleted
}
