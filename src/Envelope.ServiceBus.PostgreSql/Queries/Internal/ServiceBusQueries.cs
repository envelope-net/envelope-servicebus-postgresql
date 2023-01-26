using Envelope.ServiceBus.PostgreSql.Exchange.Internal;
using Envelope.ServiceBus.PostgreSql.Messages;
using Envelope.ServiceBus.Queries;
using Marten;

namespace Envelope.ServiceBus.PostgreSql.Queries.Internal;

internal class ServiceBusQueries : IServiceBusQueries, IDisposable, IAsyncDisposable
{
	private readonly object _sessionLock = new();

	private IQuerySession? _querySession;
	private bool _disposed;

	public DocumentStore DocumentStore { get; }

	public ServiceBusQueries(DocumentStore documentStore)
	{
		DocumentStore = documentStore ?? throw new ArgumentNullException(nameof(documentStore));
	}

	private IQuerySession CreateOrGetSession()
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

	public async Task<List<IDbHost>> GetHostsAsync(CancellationToken cancellationToken = default)
	{
		var session = CreateOrGetSession();
		var result = await session.Query<DbHost>().ToListAsync(cancellationToken).ConfigureAwait(false);
		return result?.Cast<IDbHost>().ToList() ?? new List<IDbHost>();
	}

	public async Task<List<IDbHostLog>> GetHostLogsAsync(Guid hostInstanceId, CancellationToken cancellationToken = default)
	{
		var session = CreateOrGetSession();
		var result = await session.QueryAsync(new HostLogsByHostInstanceIdQuery { HostInstanceId = hostInstanceId }, cancellationToken).ConfigureAwait(false);
		return result?.Cast<IDbHostLog>().ToList() ?? new List<IDbHostLog>();
	}

	public async Task<List<IDbJob>> GetJobsAsync(Guid hostInstanceId, CancellationToken cancellationToken = default)
	{
		var session = CreateOrGetSession();
		var result = await session.QueryAsync(new JobsByHostInstanceIdQuery { HostInstanceId = hostInstanceId }, cancellationToken).ConfigureAwait(false);
		return result?.Cast<IDbJob>().ToList() ?? new List<IDbJob>();
	}

	public async Task<IDbJob?> GetJobAsync(Guid jobInstanceId, CancellationToken cancellationToken = default)
	{
		var session = CreateOrGetSession();
		return await session.QueryAsync(new JobByInstanceIdQuery { JobInstanceId = jobInstanceId }, cancellationToken);
	}

	public async Task<List<IDbJobExecution>> GetJobLatestExecutionsAsync(Guid jobInstanceId, int count = 3, CancellationToken cancellationToken = default)
	{
		var session = CreateOrGetSession();
		var result = await session.QueryAsync(new JobLatestExecutionsByJobInstanceIdQuery { JobInstanceId = jobInstanceId, Count = count }, cancellationToken).ConfigureAwait(false);
		return result?.Cast<IDbJobExecution>().ToList() ?? new List<IDbJobExecution>();
	}

	public async Task<List<IDbJobExecution>> GetJobExecutionsAsync(Guid jobInstanceId, DateTime from, DateTime to, CancellationToken cancellationToken = default)
	{
		var session = CreateOrGetSession();
		var result = await session.QueryAsync(new JobExecutionsByJobInstanceIdQuery { JobInstanceId = jobInstanceId, From = from, To = to }, cancellationToken).ConfigureAwait(false);
		return result?.Cast<IDbJobExecution>().ToList() ?? new List<IDbJobExecution>();
	}

	public async Task<IDbJobExecution?> GetJobExecutionAsync(Guid executionId, CancellationToken cancellationToken = default)
	{
		var session = CreateOrGetSession();
		return await session.QueryAsync(new JobExecutionByExecutionIdQuery { ExecutionId = executionId }, cancellationToken);
	}

	public async Task<List<IDbJobLog>> GetJobLogsAsync(Guid executionId, CancellationToken cancellationToken = default)
	{
		var session = CreateOrGetSession();
		var result = await session.QueryAsync(new JobLogsByExecutionIdQuery { ExecutionId = executionId }, cancellationToken).ConfigureAwait(false);
		return result?.Cast<IDbJobLog>().ToList() ?? new List<IDbJobLog>();
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
		Dispose(disposing: true);
		GC.SuppressFinalize(this);
	}
}
