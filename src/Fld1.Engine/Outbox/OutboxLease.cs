namespace Fld1.Engine.Outbox;

/// <summary>
/// Fenced ownership token for one Outbox consumer. A newly acquired lease increments
/// the durable epoch and invalidates every token issued before it.
/// </summary>
public sealed class OutboxLease
{
    private readonly byte[] _ownerId;

    public ulong Epoch { get; }
    public ReadOnlySpan<byte> OwnerId => _ownerId;

    internal OutboxLease(ulong epoch, ReadOnlySpan<byte> ownerId)
    {
        Epoch = epoch;
        _ownerId = ownerId.ToArray();
    }
}
