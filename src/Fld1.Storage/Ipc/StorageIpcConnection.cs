using System.Buffers.Binary;
using System.IO.Pipes;
using System.Security.Cryptography;
using System.Text;

namespace Fld1.Storage.Ipc;

/// <summary>
 * Identity attached to one request on an authenticated local pipe session.
 * RequestId is a per-session transport correlation value; it is not a durable
 * idempotency key and may restart from one after reconnect.
/// </summary>
public readonly record struct StorageIpcRequestContext(
    Guid ClientId,
    ulong SessionGeneration,
    ulong RequestId);

/// <summary>Handles one opaque domain request. Storage itself does not interpret request payloads.</summary>
public delegate ValueTask<ReadOnlyMemory<byte>> StorageIpcRequestHandler(
    StorageIpcRequestContext context,
    ReadOnlyMemory<byte> requestPayload,
    CancellationToken cancellationToken);

/// <summary>
/// Cross-platform local IPC listener. Each connected client receives an independent
/// session generation and a strictly increasing request-id stream.
/// </summary>
public sealed class StorageIpcServer
{
    private readonly string _pipeName;
    private readonly StorageIpcRequestHandler _handler;
    private readonly SemaphoreSlim _clientSlots;
    private int _started;

    public StorageIpcServer(
        string pipeName,
        StorageIpcRequestHandler handler,
        int maxConcurrentClients = 64)
    {
        ValidatePipeName(pipeName);
        if (maxConcurrentClients is < 1 or > 254)
            throw new ArgumentOutOfRangeException(nameof(maxConcurrentClients),
                "Storage IPC server must admit between 1 and 254 concurrent clients.");

        _pipeName = pipeName;
        _handler = handler ?? throw new ArgumentNullException(nameof(handler));
        MaxConcurrentClients = maxConcurrentClients;
        _clientSlots = new SemaphoreSlim(maxConcurrentClients, maxConcurrentClients);
    }

    public int MaxConcurrentClients { get; }

    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _started, 1) != 0)
            throw new InvalidOperationException("Storage IPC server can only be started once.");

        var clients = new List<Task>();
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var pipe = new NamedPipeServerStream(
                    _pipeName,
                    PipeDirection.InOut,
                    NamedPipeServerStream.MaxAllowedServerInstances,
                    PipeTransmissionMode.Byte,
                    PipeOptions.Asynchronous | PipeOptions.CurrentUserOnly,
                    inBufferSize: 64 * 1024,
                    outBufferSize: 64 * 1024);

                try
                {
                    await pipe.WaitForConnectionAsync(cancellationToken).ConfigureAwait(false);
                }
                catch
                {
                    await pipe.DisposeAsync().ConfigureAwait(false);
                    throw;
                }

                clients.RemoveAll(task => task.IsCompleted);
                if (!_clientSlots.Wait(0))
                {
                    // Admission is deliberately non-queued: reject excess clients
                    // before allocating a session or retaining another handler task.
                    await pipe.DisposeAsync().ConfigureAwait(false);
                    continue;
                }

                clients.Add(HandleClientWithSlotAsync(pipe, cancellationToken));
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        finally
        {
            try
            {
                await Task.WhenAll(clients).ConfigureAwait(false);
            }
            finally
            {
                _clientSlots.Dispose();
            }
        }
    }

    private async Task HandleClientWithSlotAsync(
        NamedPipeServerStream pipe,
        CancellationToken cancellationToken)
    {
        try
        {
            await HandleClientAsync(pipe, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _clientSlots.Release();
        }
    }

    private async Task HandleClientAsync(
        NamedPipeServerStream pipe,
        CancellationToken cancellationToken)
    {
        await using (pipe.ConfigureAwait(false))
        {
            try
            {
                StorageIpcFrame? first = await StorageIpcProtocol.ReadAsync(
                    pipe, cancellationToken).ConfigureAwait(false);
                if (first is null || first.Value.Type != StorageIpcFrameType.Hello ||
                    first.Value.Payload.Length != 16)
                    return;

                Guid clientId = new(first.Value.Payload.Span);
                if (clientId == Guid.Empty)
                    return;

                ulong generation = CreateSessionGeneration();
                await StorageIpcProtocol.WriteAsync(pipe,
                    new StorageIpcFrame(StorageIpcFrameType.HelloAck, 0, generation, ReadOnlyMemory<byte>.Empty),
                    cancellationToken).ConfigureAwait(false);

                ulong lastRequestId = 0;
                while (!cancellationToken.IsCancellationRequested)
                {
                    StorageIpcFrame? next = await StorageIpcProtocol.ReadAsync(
                        pipe, cancellationToken).ConfigureAwait(false);
                    if (next is null)
                        return;

                    StorageIpcFrame frame = next.Value;
                    if (frame.SessionGeneration != generation)
                        throw new InvalidDataException("Storage IPC session generation mismatch.");
                    if (frame.Type == StorageIpcFrameType.Goodbye)
                        return;
                    if (frame.Type != StorageIpcFrameType.Request || frame.RequestId <= lastRequestId)
                        throw new InvalidDataException("Storage IPC request type or request-id order is invalid.");

                    lastRequestId = frame.RequestId;
                    try
                    {
                        ReadOnlyMemory<byte> response = await _handler(
                            new StorageIpcRequestContext(clientId, generation, frame.RequestId),
                            frame.Payload,
                            cancellationToken).ConfigureAwait(false);
                        await StorageIpcProtocol.WriteAsync(pipe,
                            new StorageIpcFrame(StorageIpcFrameType.Response, frame.RequestId, generation, response),
                            cancellationToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        return;
                    }
                    catch (Exception exception)
                    {
                        byte[] error = Encoding.UTF8.GetBytes(TrimError(exception.Message));
                        await StorageIpcProtocol.WriteAsync(pipe,
                            new StorageIpcFrame(StorageIpcFrameType.Error, frame.RequestId, generation, error),
                            cancellationToken).ConfigureAwait(false);
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
            }
            catch (IOException)
            {
            }
            catch (InvalidDataException)
            {
            }
            catch (ObjectDisposedException)
            {
            }
        }
    }

    private static ulong CreateSessionGeneration()
    {
        Span<byte> bytes = stackalloc byte[8];
        ulong generation;
        do
        {
            RandomNumberGenerator.Fill(bytes);
            generation = BinaryPrimitives.ReadUInt64LittleEndian(bytes);
        } while (generation == 0);
        return generation;
    }

    private static string TrimError(string message)
    {
        const int maxCharacters = 512;
        if (string.IsNullOrEmpty(message)) return "Remote request failed.";
        return message.Length <= maxCharacters ? message : message[..maxCharacters];
    }

    internal static void ValidatePipeName(string pipeName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(pipeName);
        if (pipeName.Length > 100 ||
            pipeName.Any(character => !char.IsAsciiLetterOrDigit(character) &&
                                      character is not ('.' or '-' or '_')))
            throw new ArgumentException("Pipe name must contain 1-100 ASCII letters, digits, dots, dashes or underscores.", nameof(pipeName));
    }
}

/// <summary>
/// Serialized request/response client for a local Shared Engine session. If a request
/// is cancelled or the stream becomes ambiguous, the connection is faulted and must
/// be re-established with the same durable ClientId.
/// </summary>
public sealed class StorageIpcClientConnection : IAsyncDisposable
{
    private readonly NamedPipeClientStream _pipe;
    private readonly SemaphoreSlim _requestGate = new(1, 1);
    private readonly ulong _sessionGeneration;
    private ulong _nextRequestId;
    private int _faulted;
    private int _disposed;

    public Guid ClientId { get; }
    public ulong SessionGeneration => _sessionGeneration;

    private StorageIpcClientConnection(
        NamedPipeClientStream pipe,
        Guid clientId,
        ulong sessionGeneration)
    {
        _pipe = pipe;
        ClientId = clientId;
        _sessionGeneration = sessionGeneration;
    }

    public static async Task<StorageIpcClientConnection> ConnectAsync(
        string pipeName,
        Guid clientId,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        StorageIpcServer.ValidatePipeName(pipeName);
        if (clientId == Guid.Empty)
            throw new ArgumentException("A stable non-empty client id is required.", nameof(clientId));
        if (timeout <= TimeSpan.Zero && timeout != Timeout.InfiniteTimeSpan)
            throw new ArgumentOutOfRangeException(nameof(timeout));

        var pipe = new NamedPipeClientStream(
            ".",
            pipeName,
            PipeDirection.InOut,
            PipeOptions.Asynchronous);
        try
        {
            int timeoutMilliseconds = timeout == Timeout.InfiniteTimeSpan
                ? Timeout.Infinite
                : checked((int)Math.Ceiling(timeout.TotalMilliseconds));
            await pipe.ConnectAsync(timeoutMilliseconds, cancellationToken).ConfigureAwait(false);

            byte[] clientIdBytes = clientId.ToByteArray();
            await StorageIpcProtocol.WriteAsync(pipe,
                new StorageIpcFrame(StorageIpcFrameType.Hello, 0, 0, clientIdBytes),
                cancellationToken).ConfigureAwait(false);

            StorageIpcFrame? response = await StorageIpcProtocol.ReadAsync(
                pipe, cancellationToken).ConfigureAwait(false);
            if (response is null || response.Value.Type != StorageIpcFrameType.HelloAck ||
                response.Value.SessionGeneration == 0)
                throw new InvalidDataException("Storage IPC server returned an invalid HelloAck.");

            return new StorageIpcClientConnection(pipe, clientId, response.Value.SessionGeneration);
        }
        catch
        {
            await pipe.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    public async ValueTask<ReadOnlyMemory<byte>> RequestAsync(
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (Volatile.Read(ref _faulted) != 0)
            throw new IOException("Storage IPC connection is faulted; reconnect before sending another request.");

        await _requestGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ulong requestId = checked(++_nextRequestId);
            await StorageIpcProtocol.WriteAsync(_pipe,
                new StorageIpcFrame(StorageIpcFrameType.Request, requestId, _sessionGeneration, payload),
                cancellationToken).ConfigureAwait(false);

            StorageIpcFrame? response = await StorageIpcProtocol.ReadAsync(
                _pipe, cancellationToken).ConfigureAwait(false);
            if (response is null)
                throw new EndOfStreamException("Storage IPC server disconnected before responding.");

            StorageIpcFrame frame = response.Value;
            if (frame.SessionGeneration != _sessionGeneration || frame.RequestId != requestId)
                throw new InvalidDataException("Storage IPC response does not match the outstanding request.");
            if (frame.Type == StorageIpcFrameType.Error)
                throw new StorageIpcRemoteException(requestId, Encoding.UTF8.GetString(frame.Payload.Span));
            if (frame.Type != StorageIpcFrameType.Response)
                throw new InvalidDataException("Storage IPC server returned an unexpected frame type.");

            return frame.Payload;
        }
        catch (StorageIpcRemoteException)
        {
            throw;
        }
        catch
        {
            Interlocked.Exchange(ref _faulted, 1);
            await _pipe.DisposeAsync().ConfigureAwait(false);
            throw;
        }
        finally
        {
            _requestGate.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        await _requestGate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (Volatile.Read(ref _faulted) == 0 && _pipe.IsConnected)
            {
                try
                {
                    await StorageIpcProtocol.WriteAsync(_pipe,
                        new StorageIpcFrame(StorageIpcFrameType.Goodbye, 0, _sessionGeneration, ReadOnlyMemory<byte>.Empty))
                        .ConfigureAwait(false);
                }
                catch (IOException)
                {
                }
            }
            await _pipe.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            _requestGate.Release();
            _requestGate.Dispose();
        }
    }
}

/// <summary>Remote handler error returned for one request; the session remains usable.</summary>
public sealed class StorageIpcRemoteException : IOException
{
    public ulong RequestId { get; }

    public StorageIpcRemoteException(ulong requestId, string message)
        : base(string.IsNullOrWhiteSpace(message) ? "Remote Storage IPC request failed." : message)
        => RequestId = requestId;
}
