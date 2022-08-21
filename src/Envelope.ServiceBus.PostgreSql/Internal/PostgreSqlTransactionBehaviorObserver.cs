using Envelope.Transactions;
using Marten;

namespace Envelope.ServiceBus.PostgreSql.Internal;

internal class PostgreSqlTransactionBehaviorObserver : ITransactionBehaviorObserver
{
	private readonly IDocumentSession _documentSession;
	private bool _disposed;

	public PostgreSqlTransactionBehaviorObserver(IDocumentSession documentSession)
	{
		_documentSession = documentSession ?? throw new ArgumentNullException(nameof(documentSession));
	}

	public void Commit(ITransactionCoordinator transactionCoordinator)
		=> _documentSession.SaveChanges();

	public Task CommitAsync(ITransactionCoordinator transactionCoordinator, CancellationToken cancellationToken)
		=> _documentSession.SaveChangesAsync(cancellationToken);

	public void Rollback(ITransactionCoordinator transactionCoordinator, Exception? exception)
	{
	}

	public Task RollbackAsync(ITransactionCoordinator transactionCoordinator, Exception? exception, CancellationToken cancellationToken)
		=> Task.CompletedTask;

	public async ValueTask DisposeAsync()
	{
		if (_disposed)
			return;

		_disposed = true;

		await DisposeAsyncCoreAsync().ConfigureAwait(false);

		Dispose(disposing: false);
		GC.SuppressFinalize(this);
	}

	protected virtual ValueTask DisposeAsyncCoreAsync()
		=> _documentSession.DisposeAsync();

	protected virtual void Dispose(bool disposing)
	{
		if (_disposed)
			return;

		_disposed = true;

		if (disposing)
			_documentSession.Dispose();
	}

	public void Dispose()
	{
		Dispose(disposing: true);
		GC.SuppressFinalize(this);
	}
}
