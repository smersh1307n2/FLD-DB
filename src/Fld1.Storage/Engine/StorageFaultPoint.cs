namespace Fld1.Storage.Engine;

/// <summary>
/// Internal deterministic failure points used by Storage crash/recovery tests.
/// They are not part of the public Storage API.
/// </summary>
internal enum StorageFaultPoint
{
    AfterWalAppendBeforeFlush,
    AfterWalFlushBeforePublish,
    AfterSnapshotFileFlushBeforeRename,
    AfterSnapshotRenameBeforeManifest,
    AfterManifestFileFlushBeforeRename,
    AfterManifestRenameBeforeWalTruncate
}
