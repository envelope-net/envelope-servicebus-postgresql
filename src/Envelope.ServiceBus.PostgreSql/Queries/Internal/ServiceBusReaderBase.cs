using Envelope.ServiceBus.PostgreSql.Internal;
using Marten;

namespace Envelope.ServiceBus.PostgreSql.Queries.Internal;

internal class ServiceBusReaderBase : IDisposable, IAsyncDisposable
{
	private readonly object _sessionLock = new();

	private IQuerySession? _querySession;
	private bool _disposed;

	public DocumentStore DocumentStore { get; }

	public ServiceBusReaderBase(Guid storeKey)
	{
		DocumentStore = StoreProvider.GetStore(storeKey);
		if (DocumentStore == null)
			throw new InvalidOperationException($"{nameof(DocumentStore)} == null");
	}

	protected IQuerySession CreateOrGetSession()
	{
		if (_querySession != null)
			return _querySession;

		lock (_sessionLock)
		{
			if (_querySession != null)
				return _querySession;

			_querySession = DocumentStore.QuerySession();
		}

		return _querySession;
	}

	public async ValueTask DisposeAsync()
	{
		if (_disposed)
			return;

		_disposed = true;

		await DisposeAsyncCoreAsync().ConfigureAwait(false);

#pragma warning disable CA1816 // Dispose methods should call SuppressFinalize

		Dispose(disposing: false);
		GC.SuppressFinalize(this);

#pragma warning restore CA1816 // Dispose methods should call SuppressFinalize
	}

	protected virtual async ValueTask DisposeAsyncCoreAsync()
	{
		if (_querySession != null)
			await _querySession.DisposeAsync();
	}

	protected virtual void Dispose(bool disposing)
	{
		if (_disposed)
			return;

		_disposed = true;

		if (disposing)
			_querySession?.Dispose();
	}

	public void Dispose()
	{
#pragma warning disable CA1816 // Dispose methods should call SuppressFinalize

		Dispose(disposing: true);
		GC.SuppressFinalize(this);

#pragma warning restore CA1816 // Dispose methods should call SuppressFinalize
	}
}
