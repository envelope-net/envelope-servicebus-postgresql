using Envelope.Transactions;
using Marten;
using Marten.Services;
using Npgsql;

namespace Envelope.ServiceBus.PostgreSql.Internal;

internal class PostgreSqlTransactionDocumentSessionCache : ITransactionCache, IDisposable, IAsyncDisposable
{
	private readonly object _lock = new();
	private readonly object _sessionLock = new();

	private IDocumentSession? _documentSession;
	private bool _disposed;

	public ITransactionCoordinator TransactionCoordinator { get; private set; }
	public TransactionResult TransactionResult { get; private set; }
	public string? RollbackErrorInfo { get; private set; }
	public DocumentStore DocumentStore { get; }
	
	public PostgreSqlTransactionDocumentSessionCache(DocumentStore documentStore)
	{
		DocumentStore = documentStore ?? throw new ArgumentNullException(nameof(documentStore));
		TransactionCoordinator = null!;
	}
	
	public void SetTransactionCoordinatorInternal(ITransactionCoordinator transactionCoordinator)
	{
		TransactionCoordinator = transactionCoordinator ?? throw new ArgumentNullException(nameof(transactionCoordinator));
	}

	public void ScheduleCommit()
	{
		lock (_lock)
		{
			if (TransactionResult != TransactionResult.Rollback)
				TransactionResult = TransactionResult.Commit;
		}
	}

	public void ScheduleRollback(string? rollbackErrorInfo = null)
	{
		lock (_lock)
		{
			TransactionResult = TransactionResult.Rollback;
			RollbackErrorInfo = rollbackErrorInfo;
		}
	}

	public IDocumentSession CreateOrGetSession()
	{
		if (_documentSession != null)
			return _documentSession;

		lock (_sessionLock)
		{
			if (_documentSession != null)
				return _documentSession;

			_documentSession = DocumentStore.OpenSession();
			var observer = new PostgreSqlTransactionBehaviorObserver(_documentSession);
			TransactionCoordinator.ConnectTransactionObserver(observer);
		}

		return _documentSession;
	}

	public IDocumentSession CreateOrGetSession(string connectionString)
	{
		if (string.IsNullOrWhiteSpace(connectionString))
			throw new ArgumentNullException(nameof(connectionString));

		if (_documentSession != null)
			return _documentSession;

		lock (_sessionLock)
		{
			if (_documentSession != null)
				return _documentSession;

			var options = SessionOptions.ForConnectionString(connectionString);
			_documentSession = DocumentStore.OpenSession(options);

			var observer = new PostgreSqlTransactionBehaviorObserver(_documentSession);
			TransactionCoordinator.ConnectTransactionObserver(observer);
		}

		return _documentSession;
	}

	public IDocumentSession CreateOrGetSession(NpgsqlConnection connection)
	{
		if (connection == null)
			throw new ArgumentNullException(nameof(connection));

		if (_documentSession != null)
			return _documentSession;

		lock (_sessionLock)
		{
			if (_documentSession != null)
				return _documentSession;

			var options = SessionOptions.ForConnection(connection);
			_documentSession = DocumentStore.OpenSession(options);

			var observer = new PostgreSqlTransactionBehaviorObserver(_documentSession);
			TransactionCoordinator.ConnectTransactionObserver(observer);
		}

		return _documentSession;
	}

	public IDocumentSession CreateOrGetSession(NpgsqlTransaction transaction, bool shouldAutoCommit = false)
	{
		if (transaction == null)
			throw new ArgumentNullException(nameof(transaction));

		if (_documentSession != null)
			return _documentSession;

		lock (_sessionLock)
		{
			if (_documentSession != null)
				return _documentSession;

			var options = SessionOptions.ForTransaction(transaction, shouldAutoCommit);
			_documentSession = DocumentStore.OpenSession(options);

			var observer = new PostgreSqlTransactionBehaviorObserver(_documentSession);
			TransactionCoordinator.ConnectTransactionObserver(observer);
		}

		return _documentSession;
	}

	public IDocumentSession CreateNewSession()
	{
		var session = DocumentStore.OpenSession();
		return session;
	}

	public IDocumentSession CreateNewSession(string connectionString)
	{
		if (string.IsNullOrWhiteSpace(connectionString))
			throw new ArgumentNullException(nameof(connectionString));

		var options = SessionOptions.ForConnectionString(connectionString);
		var session = DocumentStore.OpenSession(options);

		return session;
	}

	public IDocumentSession CreateNewSession(NpgsqlConnection connection)
	{
		if (connection == null)
			throw new ArgumentNullException(nameof(connection));

		var options = SessionOptions.ForConnection(connection);
		var session = DocumentStore.OpenSession(options);

		return session;
	}

	public IDocumentSession CreateNewSession(NpgsqlTransaction transaction, bool shouldAutoCommit = false)
	{
		if (transaction == null)
			throw new ArgumentNullException(nameof(transaction));

		var options = SessionOptions.ForTransaction(transaction, shouldAutoCommit);
		var session = DocumentStore.OpenSession(options);

		return session;
	}

	public async ValueTask DisposeAsync()
	{
		if (_disposed)
			return;

		_disposed = true;

		await DisposeAsyncCoreAsync().ConfigureAwait(false);

		Dispose(disposing: false);
		GC.SuppressFinalize(this);
	}

	protected virtual async ValueTask DisposeAsyncCoreAsync()
	{
		if (_documentSession != null)
			await _documentSession.DisposeAsync();

#if NETSTANDARD2_0 || NETSTANDARD2_1
		TransactionCoordinator.Dispose();
#elif NET6_0_OR_GREATER
		await TransactionCoordinator.DisposeAsync();
#endif
	}

	protected virtual void Dispose(bool disposing)
	{
		if (_disposed)
			return;

		_disposed = true;

		if (disposing)
		{
			_documentSession?.Dispose();
			TransactionCoordinator.Dispose();
		}
	}

	public void Dispose()
	{
		Dispose(disposing: true);
		GC.SuppressFinalize(this);
	}
}
