using System.Buffers.Binary;
using Fld1.Core.Hash;
using Fld1.Run.Format;

namespace Fld1.Run.Verify;

public enum RunVerifyStatus
{
    Ok = 0,
    TooSmall,
    BadMagic,
    BadBinaryVersion,
    NonZeroReserved,
    BadFlagsLayout,
    BadAlignment,
    BadFooter,
    BadNamespace,
    CorruptCounts,
}

public readonly struct RunVerifyResult
{
    public readonly RunVerifyStatus Status;
    public readonly string? Detail;

    public RunVerifyResult(RunVerifyStatus status, string? detail = null)
    {
        Status = status;
        Detail = detail;
    }

    public bool IsOk => Status == RunVerifyStatus.Ok;
    public static RunVerifyResult Ok() => new(RunVerifyStatus.Ok);
    public static RunVerifyResult Fail(RunVerifyStatus s, string? d = null) => new(s, d);
}

public static class RunVerify
{
    public static RunVerifyResult Verify(ReadOnlySpan<byte> file)
    {
        if (file.Length < RunConstants.EmptyRunSize)
            return RunVerifyResult.Fail(RunVerifyStatus.TooSmall, $"len={file.Length}");

        if (!RunHeader.CheckMagic(file))
            return RunVerifyResult.Fail(RunVerifyStatus.BadMagic);

        ushort ver = RunHeader.ReadBinaryVersion(file);
        if (ver != RunConstants.BinaryVersion)
            return RunVerifyResult.Fail(RunVerifyStatus.BadBinaryVersion, $"ver={ver}");

        uint ns = RunHeader.ReadNamespaceId(file);
        if (ns > 0xFFFF)
            return RunVerifyResult.Fail(RunVerifyStatus.BadNamespace, $"ns={ns}");

        var pad = RunHeader.ReservedPad(file);
        for (int i = 0; i < pad.Length; i++)
        {
            if (pad[i] != 0)
                return RunVerifyResult.Fail(RunVerifyStatus.NonZeroReserved, $"pad[{i}]");
        }

        ulong footerOff = RunHeader.ReadFooterOff(file);
        if (footerOff % 8 != 0)
            return RunVerifyResult.Fail(RunVerifyStatus.BadAlignment, "footerOff");
        if (footerOff + 8 != (ulong)file.Length)
            return RunVerifyResult.Fail(RunVerifyStatus.BadAlignment, $"N={file.Length} footerOff={footerOff}");

        var flags = RunHeader.ReadFlags(file);
        ulong valuesOff = RunHeader.ReadValuesOff(file);
        ulong valuesLen = RunHeader.ReadValuesLen(file);

        if ((flags & RunFlags.HasValues) == 0)
        {
            if (valuesOff != footerOff || valuesLen != 0)
                return RunVerifyResult.Fail(RunVerifyStatus.BadFlagsLayout, "HAS_VALUES=0 layout");
        }
        else
        {
            if (valuesOff + valuesLen != footerOff)
                return RunVerifyResult.Fail(RunVerifyStatus.BadFlagsLayout, "values span");
            if (valuesOff % 8 != 0 || valuesLen % 8 != 0)
                return RunVerifyResult.Fail(RunVerifyStatus.BadAlignment, "values");
        }

        if (file.Length < (int)footerOff + 8)
            return RunVerifyResult.Fail(RunVerifyStatus.TooSmall, "footer");
        ulong expected = Hashes.XxHash64(file.Slice(0, (int)footerOff));
        ulong actual = BinaryPrimitives.ReadUInt64LittleEndian(file.Slice((int)footerOff, 8));
        if (expected != actual)
            return RunVerifyResult.Fail(RunVerifyStatus.BadFooter, $"xxhash expected={expected:X} actual={actual:X}");

        return RunVerifyResult.Ok();
    }
}
