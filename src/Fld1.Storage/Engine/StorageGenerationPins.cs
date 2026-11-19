namespace Fld1.Storage.Engine;

/// <summary>
/// Tracks snapshot/cursor pins for immutable Storage generations. Retired generations
/// reject new pins and become reclaimable only after the last existing pin is released.
/// </summary>
internal sealed class StorageGenerationPins
{
    private readonly object _gate = new();
    private readonly Dictionary<ulong, GenerationState> _generations = new();

    public void Register(ulong generation)
    {
        if (generation == 0) throw new ArgumentOutOfRangeException(nameof(generation));
        lock (_gate)
        {
            if (!_generations.TryAdd(generation, new GenerationState()))
                throw new InvalidOperationException($"Storage generation {generation} is already registered.");
        }
    }

    public StorageGenerationPin Acquire(ulong generation)
    {
        lock (_gate)
        {
            if (!_generations.TryGetValue(generation, out GenerationState? state))
                throw new InvalidOperationException($"Storage generation {generation} is not registered.");
            if (state.Retired)
                throw new InvalidOperationException($"Storage generation {generation} is retired.");
            checked { state.PinCount++; }
            return new StorageGenerationPin(this, generation);
        }
    }

    /// <returns>True when the generation has no pins and can be reclaimed now.</returns>
    public bool Retire(ulong generation)
    {
        lock (_gate)
        {
            if (!_generations.TryGetValue(generation, out GenerationState? state))
                throw new InvalidOperationException($"Storage generation {generation} is not registered.");
            state.Retired = true;
            return state.PinCount == 0;
        }
    }

    public int GetPinCount(ulong generation)
    {
        lock (_gate)
            return _generations.TryGetValue(generation, out GenerationState? state)
                ? state.PinCount
                : throw new InvalidOperationException($"Storage generation {generation} is not registered.");
    }

    /// <summary>Atomically removes and returns one retired generation whose pins drained.</summary>
    public bool TryTakeReclaimable(out ulong generation)
    {
        lock (_gate)
        {
            ulong candidate = 0;
            foreach ((ulong id, GenerationState state) in _generations)
            {
                if (state.Retired && state.PinCount == 0)
                {
                    candidate = id;
                    break;
                }
            }

            if (candidate != 0)
            {
                _generations.Remove(candidate);
                generation = candidate;
                return true;
            }
        }

        generation = 0;
        return false;
    }

    internal void Release(ulong generation)
    {
        lock (_gate)
        {
            if (!_generations.TryGetValue(generation, out GenerationState? state) || state.PinCount <= 0)
                throw new InvalidOperationException("Storage generation pin count underflow or stale pin.");
            state.PinCount--;
        }
    }

    private sealed class GenerationState
    {
        public int PinCount { get; set; }
        public bool Retired { get; set; }
    }
}

internal sealed class StorageGenerationPin : IDisposable
{
    private StorageGenerationPins? _owner;
    public ulong Generation { get; }

    internal StorageGenerationPin(StorageGenerationPins owner, ulong generation)
    {
        _owner = owner;
        Generation = generation;
    }

    public void Dispose()
    {
        StorageGenerationPins? owner = Interlocked.Exchange(ref _owner, null);
        owner?.Release(Generation);
    }
}
