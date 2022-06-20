using Envelope.Transactions;
using Marten;

namespace Envelope.ServiceBus.PostgreSql.Internal;

internal class PostgreSqlTransactionBehaviorObserver : ITransactionBehaviorObserver
{
	private readonly IDocumentSession _documentSession;
	private bool disposed;

	public PostgreSqlTransactionBehaviorObserver(IDocumentSession documentSession)
	{
		_documentSession = documentSession ?? throw new ArgumentNullException(nameof(documentSession));
	}

	public void Commit(ITransactionManager transactionManager)
		=> _documentSession.SaveChanges();

	public Task CommitAsync(ITransactionManager transactionManager, CancellationToken cancellationToken)
		=> _documentSession.SaveChangesAsync(cancellationToken);

	public void Rollback(ITransactionManager transactionManager, Exception? exception)
	{
	}

	public Task RollbackAsync(ITransactionManager transactionManager, Exception? exception, CancellationToken cancellationToken)
		=> Task.CompletedTask;

	public async ValueTask DisposeAsync()
	{
		await DisposeAsyncCoreAsync().ConfigureAwait(false);

		Dispose(disposing: false);
		GC.SuppressFinalize(this);
	}

	protected virtual ValueTask DisposeAsyncCoreAsync()
		=> _documentSession.DisposeAsync();

	protected virtual void Dispose(bool disposing)
	{
		if (!disposed)
		{
			if (disposing)
				_documentSession.Dispose();

			disposed = true;
		}
	}

	public void Dispose()
	{
		Dispose(disposing: true);
		GC.SuppressFinalize(this);
	}
}
