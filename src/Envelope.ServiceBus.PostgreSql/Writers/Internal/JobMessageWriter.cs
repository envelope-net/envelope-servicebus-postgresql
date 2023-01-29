using Envelope.Logging;
using Envelope.Logging.Extensions;
using Envelope.ServiceBus.Messages;
using Envelope.ServiceBus.PostgreSql.Internal;
using Envelope.ServiceBus.Writers;
using Envelope.Trace;
using Envelope.Transactions;
using Marten;
using Microsoft.Extensions.Logging;

namespace Envelope.ServiceBus.PostgreSql.Writers.Internal;

internal class JobMessageWriter : IJobMessageWriter, IJobMessagePublisher
{
	private readonly DocumentStore _store;
	//private readonly IApplicationContext _applicationContext;
	private readonly ILogger _logger;

	public JobMessageWriter(Guid storeKey, /*IApplicationContext applicationContext, */ ILogger<JobMessageWriter> logger)
	{
		_logger = logger ?? throw new ArgumentNullException(nameof(logger));
		//_applicationContext = applicationContext ?? throw new ArgumentNullException(nameof(applicationContext));
		_store = StoreProvider.GetStore(storeKey);
	}

	public async Task WriteActiveJobMessageAsync(
		IJobMessage message,
		ITraceInfo traceInfo,
		ITransactionController? transactionController,
		CancellationToken cancellationToken = default)
	{
		if (message == null)
			throw new ArgumentNullException(nameof(message));

		if (traceInfo == null)
			throw new ArgumentNullException(nameof(traceInfo));

		traceInfo = TraceInfo.Create(traceInfo);

		if (message is not Messages.DbActiveJobMessage activeJobMessage)
		{
			activeJobMessage = new Messages.DbActiveJobMessage();
			activeJobMessage.CopyFrom(message);
		}

		try
		{
			if (transactionController == null)
			{
				await using var martenSession = _store.OpenSession();
				martenSession.Store(activeJobMessage);
				await martenSession.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
			}
			else
			{
				var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
				var martenSession = tc.CreateOrGetSession();
				martenSession.Store(activeJobMessage);
			}

		}
		catch (Exception ex)
		{
			_logger.LogErrorMessage(LogMessage.CreateErrorMessage(TraceInfo.Create(traceInfo), x => x.ExceptionInfo(ex)), true);
			throw;
		}
	}

	public Task WriteCompleteAsync(
		IJobMessage message,
		ITraceInfo traceInfo,
		ITransactionController? transactionController,
		Dictionary<string, object?>? properties = null,
		string? detail = null,
		bool? isDetailJson = null,
		CancellationToken cancellationToken = default)
	{
		if (message == null)
			throw new ArgumentNullException(nameof(message));

		if (traceInfo == null)
			throw new ArgumentNullException(nameof(traceInfo));

		traceInfo = TraceInfo.Create(traceInfo);
		message.Complete(traceInfo, properties, detail, isDetailJson);
		return WriteActiveJobMessageAsync(message, traceInfo, transactionController, cancellationToken);
	}

	public Task WriteSetErrorRetryAsync(
		IJobMessage message,
		ITraceInfo traceInfo,
		DateTime? delayedToUtc,
		TimeSpan? delay,
		int maxRetryCount,
		ITransactionController? transactionController,
		Dictionary<string, object?>? properties = null,
		string? detail = null,
		bool? isDetailJson = null,
		CancellationToken cancellationToken = default)
	{
		if (message == null)
			throw new ArgumentNullException(nameof(message));

		if (traceInfo == null)
			throw new ArgumentNullException(nameof(traceInfo));

		traceInfo = TraceInfo.Create(traceInfo);
		message.SetErrorRetry(traceInfo, delayedToUtc, delay, maxRetryCount, properties, detail, isDetailJson);
		return WriteActiveJobMessageAsync(message, traceInfo, transactionController, cancellationToken);
	}

	public Task WriteSusspendAsync(
		IJobMessage message,
		ITraceInfo traceInfo,
		ITransactionController? transactionController,
		Dictionary<string, object?>? properties = null,
		string? detail = null,
		bool? isDetailJson = null,
		CancellationToken cancellationToken = default)
	{
		if (message == null)
			throw new ArgumentNullException(nameof(message));

		if (traceInfo == null)
			throw new ArgumentNullException(nameof(traceInfo));

		traceInfo = TraceInfo.Create(traceInfo);
		message.Susspend(traceInfo, properties, detail, isDetailJson);
		return WriteActiveJobMessageAsync(message, traceInfo, transactionController, cancellationToken);
	}

	public Task WriteResumeAsync(
		IJobMessage message,
		ITraceInfo traceInfo,
		ITransactionController? transactionController,
		Dictionary<string, object?>? properties = null,
		string? detail = null,
		bool? isDetailJson = null,
		CancellationToken cancellationToken = default)
	{
		if (message == null)
			throw new ArgumentNullException(nameof(message));

		if (traceInfo == null)
			throw new ArgumentNullException(nameof(traceInfo));

		traceInfo = TraceInfo.Create(traceInfo);
		message.Resume(traceInfo, properties, detail, isDetailJson);
		return WriteActiveJobMessageAsync(message, traceInfo, transactionController, cancellationToken);
	}

	public Task WriteDeleteAsync(
		IJobMessage message,
		ITraceInfo traceInfo,
		ITransactionController? transactionController,
		Dictionary<string, object?>? properties = null,
		string? detail = null,
		bool? isDetailJson = null,
		CancellationToken cancellationToken = default)
	{
		if (message == null)
			throw new ArgumentNullException(nameof(message));

		if (traceInfo == null)
			throw new ArgumentNullException(nameof(traceInfo));

		traceInfo = TraceInfo.Create(traceInfo);
		message.Delete(traceInfo, properties, detail, isDetailJson);
		return WriteActiveJobMessageAsync(message, traceInfo, transactionController, cancellationToken);
	}

	public async Task WriteArchiveJobMessageAsync(
		IJobMessage message,
		ITraceInfo traceInfo,
		ITransactionController? transactionController,
		CancellationToken cancellationToken = default)
	{
		if (message == null)
			throw new ArgumentNullException(nameof(message));

		if (traceInfo == null)
			throw new ArgumentNullException(nameof(traceInfo));

		traceInfo = TraceInfo.Create(traceInfo);

		if (message is not Messages.DbArchivedJobMessage archivedJobMessage)
		{
			archivedJobMessage = new Messages.DbArchivedJobMessage();
			archivedJobMessage.CopyFrom(message);
		}

		try
		{
			if (transactionController == null)
			{
				await using var martenSession = _store.OpenSession();
				martenSession.Store(archivedJobMessage);
				await martenSession.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
			}
			else
			{
				var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
				var martenSession = tc.CreateOrGetSession();
				martenSession.Store(archivedJobMessage);
			}

		}
		catch (Exception ex)
		{
			_logger.LogErrorMessage(LogMessage.CreateErrorMessage(TraceInfo.Create(traceInfo), x => x.ExceptionInfo(ex)), true);
			throw;
		}
	}

	public Task PublishJobMessageAsync(
		ITraceInfo traceInfo,
		int jobMessageTypeId,
		ITransactionController? transactionController,
		Guid? taskCorrelationId = null,
		int priority = 100,
		DateTime? timeToLive = null,
		string? entityName = null,
		Guid? entityId = null,
		Dictionary<string, object?>? properties = null,
		string? detail = null,
		bool? isDetailJson = null,
		CancellationToken cancellationToken = default)
	{
		if (traceInfo == null)
			throw new ArgumentNullException(nameof(traceInfo));

		traceInfo = TraceInfo.Create(traceInfo);

		var message = JobMessageFactory.Create(
			traceInfo,
			jobMessageTypeId,
			taskCorrelationId,
			priority,
			timeToLive,
			entityName,
			entityId,
			properties,
			detail,
			isDetailJson);

		return WriteActiveJobMessageAsync(message, traceInfo, transactionController, cancellationToken);
	}
}
