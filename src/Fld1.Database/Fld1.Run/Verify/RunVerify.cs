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
        if (ver is not (RunConstants.BinaryVersion or RunConstants.ProvenanceBinaryVersion))
            return RunVerifyResult.Fail(RunVerifyStatus.BadBinaryVersion, $"ver={ver}");

        uint ns = RunHeader.ReadNamespaceId(file);
        if (ns > 0xFFFF)
            return RunVerifyResult.Fail(RunVerifyStatus.BadNamespace, $"ns={ns}");

        var pad = RunHeader.ReservedPad(file);
        int reservedStart = 0;
        if (ver == RunConstants.ProvenanceBinaryVersion)
        {
            reservedStart = RunHeader.ProvenanceHeaderBytes;
            for (int i = reservedStart; i < pad.Length; i++)
            {
                if (pad[i] != 0)
                    return RunVerifyResult.Fail(RunVerifyStatus.NonZeroReserved, $"provenance pad[{i}]");
            }
        }
        else
        {
            for (int i = 0; i < pad.Length; i++)
            {
                if (pad[i] != 0)
                    return RunVerifyResult.Fail(RunVerifyStatus.NonZeroReserved, $"pad[{i}]");
            }
        }

        ulong footerOff = RunHeader.ReadFooterOff(file);
        if (footerOff % 8 != 0)
            return RunVerifyResult.Fail(RunVerifyStatus.BadAlignment, "footerOff");
        if (footerOff + 8 != (ulong)file.Length)
            return RunVerifyResult.Fail(RunVerifyStatus.BadAlignment, $"N={file.Length} footerOff={footerOff}");

        var flags = RunHeader.ReadFlags(file);
        ulong valuesOff = RunHeader.ReadValuesOff(file);
        ulong valuesLen = RunHeader.ReadValuesLen(file);
        ulong provenanceOff = footerOff;
        ulong provenanceLen = 0;
        uint provenanceCount = 0;

        if (ver == RunConstants.ProvenanceBinaryVersion)
        {
            provenanceOff = RunHeader.ReadProvenanceOff(file);
            provenanceLen = RunHeader.ReadProvenanceLen(file);
            provenanceCount = RunHeader.ReadProvenanceCount(file);
            uint entrySize = RunHeader.ReadProvenanceEntrySize(file);
            if (entrySize != RunConstants.ProvenanceEntrySize ||
                provenanceCount > (uint)(file.Length / RunConstants.ProvenanceEntrySize) ||
                provenanceLen != (ulong)provenanceCount * RunConstants.ProvenanceEntrySize ||
                provenanceOff % 8 != 0 ||
                provenanceOff > footerOff ||
                provenanceLen != footerOff - provenanceOff)
                return RunVerifyResult.Fail(RunVerifyStatus.BadFlagsLayout, "provenance section bounds/size");
        }

        ulong expectedValueEnd = ver == RunConstants.ProvenanceBinaryVersion
            ? provenanceOff
            : footerOff;
        if ((flags & RunFlags.HasValues) == 0)
        {
            if (valuesOff != expectedValueEnd || valuesLen != 0)
                return RunVerifyResult.Fail(RunVerifyStatus.BadFlagsLayout, "HAS_VALUES=0 layout");
        }
        else
        {
            if (valuesOff > expectedValueEnd || valuesLen != expectedValueEnd - valuesOff)
                return RunVerifyResult.Fail(RunVerifyStatus.BadFlagsLayout, "values span");
            if (valuesOff % 8 != 0 || valuesLen % 8 != 0)
                return RunVerifyResult.Fail(RunVerifyStatus.BadAlignment, "values");
        }

        if (ver == RunConstants.ProvenanceBinaryVersion)
        {
            int provenanceResult = VerifyProvenance(file, provenanceOff, provenanceCount);
            if (provenanceResult >= 0)
                return RunVerifyResult.Fail(RunVerifyStatus.CorruptCounts, $"provenance entry {provenanceResult}");
        }

        if (file.Length < (int)footerOff + 8)
            return RunVerifyResult.Fail(RunVerifyStatus.TooSmall, "footer");
        ulong expected = Hashes.XxHash64(file.Slice(0, (int)footerOff));
        ulong actual = BinaryPrimitives.ReadUInt64LittleEndian(file.Slice((int)footerOff, 8));
        if (expected != actual)
            return RunVerifyResult.Fail(RunVerifyStatus.BadFooter, $"xxhash expected={expected:X} actual={actual:X}");

        return RunVerifyResult.Ok();
    }

    /// <returns>-1 when valid; otherwise the zero-based malformed row index.</returns>
    private static int VerifyProvenance(
        ReadOnlySpan<byte> file, ulong provenanceOffset, uint provenanceCount)
    {
        ReadOnlySpan<byte> previousRecord = default;
        ushort previousField = 0;
        bool havePrevious = false;
        int offset = checked((int)provenanceOffset);
        for (uint i = 0; i < provenanceCount; i++)
        {
            ReadOnlySpan<byte> row = file.Slice(
                offset + checked((int)i * RunConstants.ProvenanceEntrySize),
                RunConstants.ProvenanceEntrySize);
            ReadOnlySpan<byte> recordId = row.Slice(0, 16);
            ushort fieldId = BinaryPrimitives.ReadUInt16LittleEndian(row.Slice(16, 2));
            if (row[18] != 0 || row[19] != 0)
                return checked((int)i);

            ReadOnlySpan<byte> changeId = row.Slice(20, 16);
            byte aggregate = 0;
            foreach (byte part in changeId) aggregate |= part;
            if (aggregate == 0)
                return checked((int)i);

            uint epoch = BinaryPrimitives.ReadUInt32LittleEndian(row.Slice(36, 4));
            ulong sequence = BinaryPrimitives.ReadUInt64LittleEndian(row.Slice(40, 8));
            // Unpublished local drafts carry a stable author epoch and ChangeId
            // before the publisher assigns their per-author sequence.
            if (epoch == 0 && sequence != 0)
                return checked((int)i);

            if (havePrevious)
            {
                int cmp = recordId.SequenceCompareTo(previousRecord);
                if (cmp < 0 || (cmp == 0 && fieldId <= previousField))
                    return checked((int)i);
            }

            previousRecord = recordId;
            previousField = fieldId;
            havePrevious = true;
        }
        return -1;
    }
}
