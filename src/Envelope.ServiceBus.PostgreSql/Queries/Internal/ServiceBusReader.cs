using Envelope.ServiceBus.Messages;
using Envelope.ServiceBus.PostgreSql.Internal;
using Envelope.ServiceBus.PostgreSql.Messages;
using Envelope.ServiceBus.Queries;
using Envelope.Transactions;
using Marten;

namespace Envelope.ServiceBus.PostgreSql.Queries.Internal;

internal class ServiceBusReader : ServiceBusReaderBase, IServiceBusReader, IJobMessageReader, IDisposable, IAsyncDisposable
{
	private const int _idle = (int)JobMessageStatus.Idle;
	private const int _completed = (int)JobMessageStatus.Completed;
	private const int _error = (int)JobMessageStatus.Error;
	private const int _suspended = (int)JobMessageStatus.Suspended;
	private const int _deleted = (int)JobMessageStatus.Deleted;

	public ServiceBusReader(Guid storeKey)
		: base(storeKey)
	{
	}

	public async Task<List<IDbHost>> GetHostsAsync(CancellationToken cancellationToken = default)
	{
		var martenSession = CreateOrGetSession();
		var result = await martenSession.Query<DbHost>()
			.OrderBy(x => x.HostInfo.HostName)
			.ToListAsync(cancellationToken);

		return result?.Cast<IDbHost>().ToList() ?? new List<IDbHost>();
	}

	public async Task<List<IDbHostLog>> GetHostLogsAsync(Guid hostInstanceId, CancellationToken cancellationToken = default)
	{
		var martenSession = CreateOrGetSession();
		var result = await martenSession.Query<DbHostLog>()
			.Where(x => x.HostInstanceId == hostInstanceId)
			.OrderByDescending(x => x.CreatedUtc)
			.ToListAsync(cancellationToken);

		return result?.Cast<IDbHostLog>().ToList() ?? new List<IDbHostLog>();
	}

	public async Task<List<IDbJob>> GetJobsAsync(Guid hostInstanceId, CancellationToken cancellationToken = default)
	{
		var martenSession = CreateOrGetSession();
		var result = await martenSession.Query<DbJob>()
			.Where(x => x.HostInstanceId == hostInstanceId)
			.ToListAsync(cancellationToken);

		return result?.Cast<IDbJob>().ToList() ?? new List<IDbJob>();
	}

	public async Task<List<IDbJob>> GetJobsAsync(string jobName, string hostName, int page =1, int pageSize = 5, CancellationToken cancellationToken = default)
	{
		var martenSession = CreateOrGetSession();
		var result = await martenSession.Query<DbJob>()
			.Where(x => x.Name == jobName
				&& (hostName == null || x.HostName == hostName))
			.OrderByDescending(x => x.LastUpdateUtc)
			.Skip((page - 1) * pageSize)
			.Take(pageSize)
			.ToListAsync(cancellationToken);

		return result?.Cast<IDbJob>().ToList() ?? new List<IDbJob>();
	}

	public async Task<IDbJob?> GetJobAsync(Guid jobInstanceId, CancellationToken cancellationToken = default)
	{
		var martenSession = CreateOrGetSession();
		return await martenSession.Query<DbJob>()
			.Where(x => x.JobInstanceId == jobInstanceId)
			.FirstOrDefaultAsync(cancellationToken);
	}

	public async Task<List<IDbJobExecution>> GetJobLatestExecutionsAsync(Guid jobInstanceId, int page = 1, int pageSize = 3, CancellationToken cancellationToken = default)
	{
		var martenSession = CreateOrGetSession();
		var result = await martenSession.Query<DbJobExecution>()
			.Where(x => x.JobInstanceId == jobInstanceId)
			.OrderByDescending(x => x.StartedUtc)
			.Skip((page - 1) * pageSize)
			.Take(pageSize)
			.ToListAsync(cancellationToken);

		return result?.Cast<IDbJobExecution>().ToList() ?? new List<IDbJobExecution>();
	}

	public async Task<List<IDbJobExecution>> GetJobExecutionsAsync(Guid jobInstanceId, DateTime fromUtc, DateTime toUtc, CancellationToken cancellationToken = default)
	{
		var martenSession = CreateOrGetSession();
		var result = await martenSession.Query<DbJobExecution>()
			.Where(x => x.JobInstanceId == jobInstanceId && fromUtc <= x.StartedUtc && x.StartedUtc <= toUtc)
			.OrderByDescending(x => x.StartedUtc)
			.ToListAsync(cancellationToken);

		return result?.Cast<IDbJobExecution>().ToList() ?? new List<IDbJobExecution>();
	}

	public async Task<IDbJobExecution?> GetJobExecutionAsync(Guid executionId, CancellationToken cancellationToken = default)
	{
		var martenSession = CreateOrGetSession();
		return await martenSession.Query<DbJobExecution>()
			.Where(x => x.ExecutionId == executionId)
			.FirstOrDefaultAsync(cancellationToken);
	}

	public async Task<List<IDbJobLog>> GetJobLogsAsync(Guid executionId, CancellationToken cancellationToken = default)
	{
		var martenSession = CreateOrGetSession();
		var result = await martenSession.Query<DbJobLog>()
			.Where(x => x.ExecutionId == executionId)
			.ToListAsync(cancellationToken);

		return result?.Cast<IDbJobLog>().ToList() ?? new List<IDbJobLog>();
	}

	public async Task<List<IDbJobLog>> JobLogsForMessageAsync(Guid jobMessageId, CancellationToken cancellationToken = default)
	{
		var martenSession = CreateOrGetSession();
		var result = await martenSession.Query<DbJobLog>()
			.Where(x => x.JobMessageId == jobMessageId)
			.ToListAsync(cancellationToken);

		return result?.Cast<IDbJobLog>().ToList() ?? new List<IDbJobLog>();
	}

	public async Task<IDbJobLog?> GetJobLogAsync(Guid idLogMessage, CancellationToken cancellationToken = default)
	{
		var martenSession = CreateOrGetSession();
		return await martenSession.Query<DbJobLog>()
			.Where(x => x.IdLogMessage == idLogMessage)
			.FirstOrDefaultAsync(cancellationToken);
	}

	public async Task<IJobMessage?> GetActiveJobMessageAsync(
		Guid jobMessageId,
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

		return await martenSession.Query<DbActiveJobMessage>()
			.Where(x => x.Id == jobMessageId)
			.FirstOrDefaultAsync(cancellationToken);
	}

	public async Task<IJobMessage?> GetArchivedJobMessageAsync(
		Guid jobMessageId,
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

		return await martenSession.Query<DbArchivedJobMessage>()
			.Where(x => x.Id == jobMessageId)
			.FirstOrDefaultAsync(cancellationToken);
	}

	public async Task<List<IJobMessage>> GetActiveJobMessagesAsync(
		int jobMessageTypeId,
		int? status = null,
		int page = 1,
		int pageSize = 20,
		bool includeDeleted = false,
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

		var result = await martenSession.Query<DbActiveJobMessage>()
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
		bool includeSuspended = false,
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

		var result = includeSuspended
			? await martenSession.Query<DbActiveJobMessage>()
				.Where(x => x.LastUpdatedUtc < lastUpdatedBeforeUtc
					&& x.Status != _idle
					&& x.Status != _error)
				.ToListAsync(cancellationToken)
			: await martenSession.Query<DbActiveJobMessage>()
				.Where(x => x.LastUpdatedUtc < lastUpdatedBeforeUtc
					&& x.Status != _idle
					&& x.Status != _error
					&& x.Status != _suspended)
				.ToListAsync(cancellationToken);

		return result?.Cast<IJobMessage>().ToList() ?? new List<IJobMessage>();
	}

	public async Task<int> GetIdleActiveJobMessagesCountAsync(
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

		return await martenSession.Query<DbActiveJobMessage>()
			.Where(x => x.JobMessageTypeId == jobMessageTypeId && x.Status == _idle)
			.CountAsync(cancellationToken);
	}

	public async Task<int> GetCompletedActiveJobMessagesCountAsync(
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

		return await martenSession.Query<DbActiveJobMessage>()
			.Where(x => x.JobMessageTypeId == jobMessageTypeId && x.Status == _completed)
			.CountAsync(cancellationToken);
	}

	public async Task<int> GetErrorActiveJobMessagesCountAsync(
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

		return await martenSession.Query<DbActiveJobMessage>()
			.Where(x => x.JobMessageTypeId == jobMessageTypeId && x.Status == _error)
			.CountAsync(cancellationToken);
	}

	public async Task<int> GetSuspendedActiveJobMessagesCountAsync(
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

		return await martenSession.Query<DbActiveJobMessage>()
			.Where(x => x.JobMessageTypeId == jobMessageTypeId && x.Status == _suspended)
			.CountAsync(cancellationToken);
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

		return await martenSession.Query<DbActiveJobMessage>()
			.Where(x => x.JobMessageTypeId == jobMessageTypeId && x.Status != _deleted)
			.CountAsync(cancellationToken);
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
		bool skipSuspendedMessages = false,
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
			? (skipSuspendedMessages
				? await martenSession.Query<DbActiveJobMessage>()
					.Where(x =>
						x.JobMessageTypeId == jobMessageTypeId
						&& (!x.DelayedToUtc.HasValue || x.DelayedToUtc <= maxDelayedToUtc)
						&& (x.Status == _idle || x.Status == _error))
					.OrderBy(x => x.CreatedUtc)
					.FirstOrDefaultAsync(cancellationToken)
				: await martenSession.Query<DbActiveJobMessage>()
					.Where(x =>
						x.JobMessageTypeId == jobMessageTypeId
						&& (!x.DelayedToUtc.HasValue || x.DelayedToUtc <= maxDelayedToUtc)
						&& (x.Status == _idle || x.Status == _error || x.Status == _suspended))
					.OrderBy(x => x.CreatedUtc)
					.FirstOrDefaultAsync(cancellationToken))
			: (skipSuspendedMessages
				? await martenSession.Query<DbActiveJobMessage>()
					.Where(x =>
						x.JobMessageTypeId == jobMessageTypeId
						&& (x.Status == _idle || x.Status == _error))
					.OrderBy(x => x.CreatedUtc)
					.FirstOrDefaultAsync(cancellationToken)
				: await martenSession.Query<DbActiveJobMessage>()
					.Where(x =>
						x.JobMessageTypeId == jobMessageTypeId
						&& (x.Status == _idle || x.Status == _error || x.Status == _suspended))
					.OrderBy(x => x.CreatedUtc)
					.FirstOrDefaultAsync(cancellationToken));
	}

	public async Task<List<IJobMessage>> GetActiveEntityJobMessagesAsync(
		string entityName,
		Guid? entityId,
		int? jobMessageTypeId,
		int? status = null,
		int page = 1,
		int pageSize = 20,
		bool includeDeleted = false,
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
				x.EntityName == entityName
				&& (!entityId.HasValue || x.EntityId == entityId)
				&& (!jobMessageTypeId.HasValue || x.JobMessageTypeId == jobMessageTypeId)
				&& (includeDeleted || x.Status != (int)JobMessageStatus.Deleted)
				&& (!status.HasValue || x.Status == status))
			.OrderBy(x => x.CreatedUtc)
			.Skip((page - 1) * pageSize)
			.Take(pageSize)
			.ToListAsync(cancellationToken);

		return result?.Cast<IJobMessage>().ToList() ?? new List<IJobMessage>();
	}
}
