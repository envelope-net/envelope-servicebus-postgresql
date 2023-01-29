using Envelope.ServiceBus.Messages;
using Envelope.ServiceBus.PostgreSql.Exchange.Internal;
using Envelope.ServiceBus.PostgreSql.Internal;
using Envelope.ServiceBus.Queries;
using Envelope.Transactions;
using Marten;

namespace Envelope.ServiceBus.PostgreSql.Queries.Internal;

internal class ServiceBusReader : IServiceBusReader, IDisposable, IAsyncDisposable
{
	private readonly object _sessionLock = new();

	private IQuerySession? _querySession;
	private bool _disposed;

	public DocumentStore DocumentStore { get; }

	public ServiceBusReader(DocumentStore documentStore)
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
		var result = await session.QueryAsync(new HostsQuery(), cancellationToken).ConfigureAwait(false);
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

	public async Task<List<IDbJob>> GetJobsAsync(string jobName, string hostName, int count = 5, CancellationToken cancellationToken = default)
	{
		var session = CreateOrGetSession();
		var result = await session.QueryAsync(new JobsByNameQuery { Name = jobName, HostName = hostName, /*Page = 1,*/ PageSize = count }, cancellationToken).ConfigureAwait(false);
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
		var result = await session.QueryAsync(new JobLatestExecutionsByJobInstanceIdQuery { JobInstanceId = jobInstanceId, /*Page = 1,*/ PageSize = count }, cancellationToken).ConfigureAwait(false);
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

	public async Task<List<IDbJobLog>> JobLogsForMessageAsync(Guid jobMessageId, CancellationToken cancellationToken = default)
	{
		var session = CreateOrGetSession();
		var result = await session.QueryAsync(new JobLogsByJobMessageIdQuery { JobMessageId = jobMessageId }, cancellationToken).ConfigureAwait(false);
		return result?.Cast<IDbJobLog>().ToList() ?? new List<IDbJobLog>();
	}

	public async Task<List<IJobMessage>> GetActiveJobMessagesAsync(
		int jobMessageTypeId,
		int? status = null,
		bool includeDeleted = false,
		int page = 1,
		int pageSize = 20,
		ITransactionController? transactionController = null,
		CancellationToken cancellationToken = default)
	{
		if (page < 1)
			page = 1;

		IQuerySession martenSession;
		if (transactionController == null)
		{
			martenSession = CreateOrGetSession();
		}
		else
		{
			var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
			martenSession = tc.CreateOrGetSession();
		}

		var result = await martenSession.Query<Messages.DbActiveJobMessage>()
			.Where(x =>
				x.JobMessageTypeId == jobMessageTypeId
				&& (includeDeleted || x.Status != (int)JobMessageStatus.Deleted)
				&& (!status.HasValue || x.Status == status))
			.OrderBy(x => x.CreatedUtc)
			.Skip((page - 1) * pageSize)
			.Take(pageSize)
			.ToListAsync(cancellationToken);

		return result?.Cast<IJobMessage>().ToList() ?? new List<IJobMessage>();
	}

	public async Task<List<IJobMessage>> GetActiveJobMessagesToArchiveAsync(
		DateTime lastUpdatedBeforeUtc,
		ITransactionController? transactionController = null,
		CancellationToken cancellationToken = default)
	{
		IQuerySession martenSession;
		if (transactionController == null)
		{
			martenSession = CreateOrGetSession();
		}
		else
		{
			var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
			martenSession = tc.CreateOrGetSession();
		}

		var result = await martenSession.QueryAsync(new ActiveJobMessagesToArchiveQuery { LastUpdatedBeforeUtc = lastUpdatedBeforeUtc }, cancellationToken).ConfigureAwait(false);
		return result?.Cast<IJobMessage>().ToList() ?? new List<IJobMessage>();
	}

	public async Task<int> GetNextActiveJobMessagesCountAsync(
		int jobMessageTypeId,
		ITransactionController? transactionController = null,
		CancellationToken cancellationToken = default)
	{
		IQuerySession martenSession;
		if (transactionController == null)
		{
			martenSession = CreateOrGetSession();
		}
		else
		{
			var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
			martenSession = tc.CreateOrGetSession();
		}

		return await martenSession.QueryAsync(new NextActiveJobMessagesCountQuery { JobMessageTypeId = jobMessageTypeId }, cancellationToken).ConfigureAwait(false);
	}

	public async Task<int> GetSusspendedActiveJobMessagesCountAsync(
		int jobMessageTypeId,
		ITransactionController? transactionController = null,
		CancellationToken cancellationToken = default)
	{
		IQuerySession martenSession;
		if (transactionController == null)
		{
			martenSession = CreateOrGetSession();
		}
		else
		{
			var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
			martenSession = tc.CreateOrGetSession();
		}

		return await martenSession.QueryAsync(new SusspendedActiveJobMessagesCountQuery { JobMessageTypeId = jobMessageTypeId }, cancellationToken).ConfigureAwait(false);
	}

	public async Task<int> GetAllActiveJobMessagesCountAsync(
		int jobMessageTypeId,
		ITransactionController? transactionController = null,
		CancellationToken cancellationToken = default)
	{
		IQuerySession martenSession;
		if (transactionController == null)
		{
			martenSession = CreateOrGetSession();
		}
		else
		{
			var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
			martenSession = tc.CreateOrGetSession();
		}

		return await martenSession.QueryAsync(new AllActiveJobMessagesCountQuery { JobMessageTypeId = jobMessageTypeId }, cancellationToken).ConfigureAwait(false);
	}

	public Task<int> GetArchivedJobMessagesCountAsync(
		int jobMessageTypeId,
		int? status = null,
		bool includeDeleted = false,
		ITransactionController? transactionController = null,
		CancellationToken cancellationToken = default)
	{
		IQuerySession martenSession;
		if (transactionController == null)
		{
			martenSession = CreateOrGetSession();
		}
		else
		{
			var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
			martenSession = tc.CreateOrGetSession();
		}

		return martenSession.Query<Messages.DbArchivedJobMessage>()
			.Where(x =>
				x.JobMessageTypeId == jobMessageTypeId
				&& (includeDeleted || x.Status != (int)JobMessageStatus.Deleted)
				&& (!status.HasValue || x.Status == status))
			.CountAsync(cancellationToken);
	}

	public async Task<List<IJobMessage>> GetArchivedJobMessagesAsync(
		int jobMessageTypeId,
		int? status = null,
		bool includeDeleted = false,
		int page = 1,
		int pageSize = 20,
		ITransactionController? transactionController = null,
		CancellationToken cancellationToken = default)
	{
		if (page < 1)
			page = 1;

		IQuerySession martenSession;
		if (transactionController == null)
		{
			martenSession = CreateOrGetSession();
		}
		else
		{
			var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
			martenSession = tc.CreateOrGetSession();
		}

		var result = await martenSession.Query<Messages.DbArchivedJobMessage>()
			.Where(x =>
				x.JobMessageTypeId == jobMessageTypeId
				&& (includeDeleted || x.Status != (int)JobMessageStatus.Deleted)
				&& (!status.HasValue || x.Status == status))
			.OrderBy(x => x.CreatedUtc)
			.Skip((page - 1) * pageSize)
			.Take(pageSize)
			.ToListAsync(cancellationToken);

		return result?.Cast<IJobMessage>().ToList() ?? new List<IJobMessage>();
	}

	public async Task<IJobMessage?> GetNextActiveJobMessageAsync(
		int jobMessageTypeId,
		DateTime? maxDelayedToUtc,
		ITransactionController? transactionController = null,
		CancellationToken cancellationToken = default)
	{
		IQuerySession martenSession;
		if (transactionController == null)
		{
			martenSession = CreateOrGetSession();
		}
		else
		{
			var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
			martenSession = tc.CreateOrGetSession();
		}

		return maxDelayedToUtc.HasValue
			? await martenSession.QueryAsync(new NextActiveJobMessageQuery { JobMessageTypeId = jobMessageTypeId, NowUtc = maxDelayedToUtc.Value }, cancellationToken).ConfigureAwait(false)
			: await martenSession.QueryAsync(new NextActiveJobMessageIgnoringDelayQuery { JobMessageTypeId = jobMessageTypeId }, cancellationToken).ConfigureAwait(false);
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
