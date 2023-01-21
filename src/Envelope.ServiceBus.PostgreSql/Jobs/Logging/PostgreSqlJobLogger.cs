using Envelope.Logging;
using Envelope.Logging.Extensions;
using Envelope.ServiceBus.Jobs;
using Envelope.ServiceBus.Jobs.Logging;
using Envelope.ServiceBus.PostgreSql.Internal;
using Envelope.ServiceBus.PostgreSql.Messages;
using Envelope.Services;
using Envelope.Trace;
using Envelope.Transactions;
using Marten;
using Microsoft.Extensions.Logging;

namespace Envelope.ServiceBus.PostgreSql.Jobs.Logging;

public class PostgreSqlJobLogger : IJobLogger
{
	private readonly DocumentStore _store;
	private readonly IApplicationContext _applicationContext;
	private readonly ILogger _logger;

	private const string RESULT_LOG_CODE = "SERVICE_RESULT";

	public PostgreSqlJobLogger(Guid storeKey, IApplicationContext applicationContext, ILogger<PostgreSqlJobLogger> logger)
	{
		_logger = logger ?? throw new ArgumentNullException(nameof(logger));
		_applicationContext = applicationContext ?? throw new ArgumentNullException(nameof(applicationContext));
		_store = StoreProvider.GetStore(storeKey);
	}

	private static Action<LogMessageBuilder> AppendToBuilder(
		Action<LogMessageBuilder> messageBuilder,
		string logCode,
		IJob job,
		JobExecuteResult executeResult,
		JobExecuteStatus? newExecuteStatus,
		string? detail)
	{
		messageBuilder += x => x
			.AddCustomData(nameof(executeResult.ExecutionId), executeResult.ExecutionId.ToString())
			.AddCustomData(nameof(executeResult.ExecuteStatus), executeResult.ExecuteStatus.ToString())
			.AddCustomData(nameof(newExecuteStatus), newExecuteStatus?.ToString())
			.AddCustomData(nameof(job.Name), job?.Name);

		if (!string.IsNullOrWhiteSpace(detail))
			messageBuilder += x => x.AddCustomData(nameof(detail), detail);

		if (!string.IsNullOrWhiteSpace(logCode))
			messageBuilder += x => x.LogCode(logCode, force: false);

		executeResult.SetStatus(newExecuteStatus);

		return messageBuilder;
	}

	private static Action<ErrorMessageBuilder> AppendToBuilder(
		Action<ErrorMessageBuilder> messageBuilder,
		string logCode,
		IJob job,
		JobExecuteResult executeResult,
		JobExecuteStatus? newExecuteStatus,
		string? detail)
	{
		messageBuilder += x => x
			.AddCustomData(nameof(executeResult.ExecutionId), executeResult.ExecutionId.ToString())
			.AddCustomData(nameof(executeResult.ExecuteStatus), executeResult.ExecuteStatus.ToString())
			.AddCustomData(nameof(newExecuteStatus), newExecuteStatus?.ToString())
			.AddCustomData(nameof(job.Name), job?.Name);

		if (!string.IsNullOrWhiteSpace(detail))
			messageBuilder += x => x.AddCustomData(nameof(detail), detail);

		if (!string.IsNullOrWhiteSpace(logCode))
			messageBuilder += x => x.LogCode(logCode, force: false);

		executeResult.SetStatus(newExecuteStatus);

		return messageBuilder;
	}

	public void LogStatus(
		ITraceInfo traceInfo,
		IJob job,
		JobExecuteResult executeResult,
		JobExecuteStatus? newExecuteStatus)
	{
		if (job == null)
			throw new ArgumentNullException(nameof(job));

		if (executeResult == null)
			throw new ArgumentNullException(nameof(executeResult));

		executeResult.SetStatus(newExecuteStatus);

		try
		{
			using var martenSession = _store.OpenSession();
			martenSession.Store(DbJob.Create(job, executeResult));
			martenSession.SaveChanges();
		}
		catch (Exception ex)
		{
			_logger.LogErrorMessage(LogMessage.CreateErrorMessage(TraceInfo.Create(traceInfo), x => x.ExceptionInfo(ex)), true);
		}
	}

	public async Task LogStatusAsync(
		ITraceInfo traceInfo,
		IJob job,
		JobExecuteResult executeResult,
		JobExecuteStatus? newExecuteStatus,
		CancellationToken cancellationToken = default)
	{
		if (job == null)
			throw new ArgumentNullException(nameof(job));

		if (executeResult == null)
			throw new ArgumentNullException(nameof(executeResult));

		executeResult.SetStatus(newExecuteStatus);

		try
		{
			await using var martenSession = _store.OpenSession();
			martenSession.Store(DbJob.Create(job, executeResult));
			await martenSession.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			_logger.LogErrorMessage(LogMessage.CreateErrorMessage(TraceInfo.Create(traceInfo), x => x.ExceptionInfo(ex)), true);
		}
	}

	public async Task LogExecutionStartAsync(
		ITraceInfo traceInfo,
		IJob job,
		JobExecuteResult executeResult,
		DateTime startedUtc,
		bool finished = false)
	{
		if (job == null)
			throw new ArgumentNullException(nameof(job));

		if (executeResult == null)
			throw new ArgumentNullException(nameof(executeResult));

		try
		{
			await using var martenSession = _store.OpenSession();
			martenSession.Store(DbJobExecution.Started(job, executeResult, startedUtc, finished));
			await martenSession.SaveChangesAsync(token: default).ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			_logger.LogErrorMessage(LogMessage.CreateErrorMessage(TraceInfo.Create(traceInfo), x => x.ExceptionInfo(ex)), true);
		}
	}

	public async Task LogExecutionFinishedAsync(
		ITraceInfo traceInfo,
		IJob job,
		JobExecuteResult executeResult,
		DateTime startedUtc)
	{
		if (job == null)
			throw new ArgumentNullException(nameof(job));

		if (executeResult == null)
			throw new ArgumentNullException(nameof(executeResult));

		try
		{
			await using var martenSession = _store.OpenSession();
			martenSession.Store(DbJobExecution.Finised(job, executeResult, startedUtc));
			await martenSession.SaveChangesAsync(token: default).ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			_logger.LogErrorMessage(LogMessage.CreateErrorMessage(TraceInfo.Create(traceInfo), x => x.ExceptionInfo(ex)), true);
		}
	}

	public async Task<ILogMessage?> LogTraceAsync(
		ITraceInfo traceInfo,
		IJob job,
		JobExecuteResult executeResult,
		JobExecuteStatus? newExecuteStatus,
		string logCode,
		Action<LogMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionController? transactionController = null,
		CancellationToken cancellationToken = default)
	{
		if (job == null)
			throw new ArgumentNullException(nameof(job));

		if (executeResult == null)
			throw new ArgumentNullException(nameof(executeResult));

		AppendToBuilder(messageBuilder, logCode, job, executeResult, newExecuteStatus, detail);
		var msg = _logger.PrepareTraceMessage(traceInfo, messageBuilder, true);
		if (msg != null)
		{
			_logger.LogTraceMessage(msg, true);

			var tmp = msg.Exception;
			msg.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization

			try
			{
				await using var martenSession = _store.OpenSession();
				martenSession.Store(DbJobLog.Create(job, executeResult, msg, detail, logCode));
				await martenSession.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				_logger.LogErrorMessage(LogMessage.CreateErrorMessage(TraceInfo.Create(traceInfo), x => x.ExceptionInfo(ex)), true);
			}

			msg.Exception = tmp;
		}

		return msg;
	}

	public async Task<ILogMessage?> LogDebugAsync(
		ITraceInfo traceInfo,
		IJob job,
		JobExecuteResult executeResult,
		JobExecuteStatus? newExecuteStatus,
		string logCode,
		Action<LogMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionController? transactionController = null,
		CancellationToken cancellationToken = default)
	{
		if (job == null)
			throw new ArgumentNullException(nameof(job));

		if (executeResult == null)
			throw new ArgumentNullException(nameof(executeResult));

		AppendToBuilder(messageBuilder, logCode, job, executeResult, newExecuteStatus, detail);
		var msg = _logger.PrepareDebugMessage(traceInfo, messageBuilder, true);
		if (msg != null)
		{
			_logger.LogDebugMessage(msg, true);

			var tmp = msg.Exception;
			msg.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization

			try
			{
				await using var martenSession = _store.OpenSession();
				martenSession.Store(DbJobLog.Create(job, executeResult, msg, detail, logCode));
				await martenSession.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				_logger.LogErrorMessage(LogMessage.CreateErrorMessage(TraceInfo.Create(traceInfo), x => x.ExceptionInfo(ex)), true);
			}

			msg.Exception = tmp;
		}

		return msg;
	}

	public async Task<ILogMessage?> LogInformationAsync(
		ITraceInfo traceInfo,
		IJob job,
		JobExecuteResult executeResult,
		JobExecuteStatus? newExecuteStatus,
		string logCode,
		Action<LogMessageBuilder> messageBuilder,
		string? detail = null,
		bool force = false,
		ITransactionController? transactionController = null,
		CancellationToken cancellationToken = default)
	{
		if (job == null)
			throw new ArgumentNullException(nameof(job));

		if (executeResult == null)
			throw new ArgumentNullException(nameof(executeResult));

		AppendToBuilder(messageBuilder, logCode, job, executeResult, newExecuteStatus, detail);
		var msg = _logger.PrepareInformationMessage(traceInfo, messageBuilder, !force);
		if (msg != null)
		{
			_logger.LogInformationMessage(msg, true);

			var tmp = msg.Exception;
			msg.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization

			try
			{
				await using var martenSession = _store.OpenSession();
				martenSession.Store(DbJobLog.Create(job, executeResult, msg, detail, logCode));
				await martenSession.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				_logger.LogErrorMessage(LogMessage.CreateErrorMessage(TraceInfo.Create(traceInfo), x => x.ExceptionInfo(ex)), true);
			}

			msg.Exception = tmp;
		}

		return msg;
	}

	public async Task<ILogMessage?> LogWarningAsync(
		ITraceInfo traceInfo,
		IJob job,
		JobExecuteResult executeResult,
		JobExecuteStatus? newExecuteStatus,
		string logCode,
		Action<LogMessageBuilder> messageBuilder,
		string? detail = null,
		bool force = false,
		ITransactionController? transactionController = null,
		CancellationToken cancellationToken = default)
	{
		if (job == null)
			throw new ArgumentNullException(nameof(job));

		if (executeResult == null)
			throw new ArgumentNullException(nameof(executeResult));

		AppendToBuilder(messageBuilder, logCode, job, executeResult, newExecuteStatus, detail);
		var msg = _logger.PrepareWarningMessage(traceInfo, messageBuilder, !force);
		if (msg != null)
		{
			_logger.LogWarningMessage(msg, true);

			var tmp = msg.Exception;
			msg.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization

			try
			{
				await using var martenSession = _store.OpenSession();
				martenSession.Store(DbJobLog.Create(job, executeResult, msg, detail, logCode));
				await martenSession.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				_logger.LogErrorMessage(LogMessage.CreateErrorMessage(TraceInfo.Create(traceInfo), x => x.ExceptionInfo(ex)), true);
			}

			msg.Exception = tmp;
		}

		return msg;
	}

	public async Task<IErrorMessage> LogErrorAsync(
		ITraceInfo traceInfo,
		IJob job,
		JobExecuteResult executeResult,
		JobExecuteStatus? newExecuteStatus,
		string logCode,
		Action<ErrorMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionController? transactionController = null,
		CancellationToken cancellationToken = default)
	{
		if (job == null)
			throw new ArgumentNullException(nameof(job));

		if (executeResult == null)
			throw new ArgumentNullException(nameof(executeResult));

		AppendToBuilder(messageBuilder, logCode, job, executeResult, newExecuteStatus, detail);
		var msg = _logger.PrepareErrorMessage(traceInfo, messageBuilder, false)!;
		_logger.LogErrorMessage(msg, true);

		var tmp = msg.Exception;
		msg.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization

		try
		{
			await using var martenSession = _store.OpenSession();
			martenSession.Store(DbJobLog.Create(job, executeResult, msg, detail, logCode));
			await martenSession.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			_logger.LogErrorMessage(LogMessage.CreateErrorMessage(TraceInfo.Create(traceInfo), x => x.ExceptionInfo(ex)), true);
		}

		msg.Exception = tmp;

		return msg;
	}

	public async Task<IErrorMessage> LogCriticalAsync(
		ITraceInfo traceInfo,
		IJob job,
		JobExecuteResult executeResult,
		JobExecuteStatus? newExecuteStatus,
		string logCode,
		Action<ErrorMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionController? transactionController = null,
		CancellationToken cancellationToken = default)
	{
		if (job == null)
			throw new ArgumentNullException(nameof(job));

		if (executeResult == null)
			throw new ArgumentNullException(nameof(executeResult));

		AppendToBuilder(messageBuilder, logCode, job, executeResult, newExecuteStatus, detail);
		var msg = _logger.PrepareCriticalMessage(traceInfo, messageBuilder, false)!;
		_logger.LogCriticalMessage(msg, true);

		var tmp = msg.Exception;
		msg.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization

		try
		{
			await using var martenSession = _store.OpenSession();
			martenSession.Store(DbJobLog.Create(job, executeResult, msg, detail, logCode));
			await martenSession.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			_logger.LogErrorMessage(LogMessage.CreateErrorMessage(TraceInfo.Create(traceInfo), x => x.ExceptionInfo(ex)), true);
		}

		msg.Exception = tmp;

		return msg;
	}

	public async Task LogResultErrorMessagesAsync(
		IJob job,
		JobExecuteResult executeResult,
		JobExecuteStatus? newExecuteStatus,
		string logCode,
		IResult result,
		ITransactionCoordinator? transactionCoordinator = null,
		CancellationToken cancellationToken = default)
	{
		if (job == null)
			throw new ArgumentNullException(nameof(job));

		if (executeResult == null)
			throw new ArgumentNullException(nameof(executeResult));

		if (result == null)
			return;

		var msgs = new List<DbJobLog>();

		foreach (var errorMessage in result.ErrorMessages)
		{
			var builder = new ErrorMessageBuilder(errorMessage);
			builder
				.AddCustomData(nameof(executeResult.ExecutionId), executeResult.ExecutionId.ToString())
				.AddCustomData(nameof(executeResult.ExecuteStatus), executeResult.ExecuteStatus.ToString())
				.AddCustomData(nameof(newExecuteStatus), newExecuteStatus?.ToString())
				.AddCustomData(nameof(job.Name), job.Name)
				.LogCode(logCode, force: true);

			executeResult.SetStatus(newExecuteStatus);

			if (errorMessage.LogLevel == LogLevel.Error)
				_logger.LogErrorMessage(errorMessage, true);
			else if (errorMessage.LogLevel == LogLevel.Critical)
				_logger.LogCriticalMessage(errorMessage, true);
			else
				throw new NotSupportedException($"{nameof(errorMessage.LogLevel)} = {errorMessage.LogLevel}");

			errorMessage.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization
			msgs.Add(DbJobLog.Create(job, executeResult, errorMessage, null, string.IsNullOrWhiteSpace(logCode) ? RESULT_LOG_CODE : logCode));
		}

		if (0 < msgs.Count)
		{
			try
			{
				await using var martenSession = _store.OpenSession();
				martenSession.Store((IEnumerable<DbJobLog>)msgs);
				await martenSession.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				_logger.LogErrorMessage(LogMessage.CreateErrorMessage(TraceInfo.Create(_applicationContext), x => x.ExceptionInfo(ex)), true);
			}
		}
	}

	public async Task LogResultAllMessagesAsync(
		IJob job,
		JobExecuteResult executeResult,
		JobExecuteStatus? newExecuteStatus,
		string logCode,
		IResult result,
		ITransactionCoordinator? transactionCoordinator = null,
		CancellationToken cancellationToken = default)
	{
		if (job == null)
			throw new ArgumentNullException(nameof(job));

		if (executeResult == null)
			throw new ArgumentNullException(nameof(executeResult));

		if (result == null)
			return;

		foreach (var msg in result.SuccessMessages)
		{
			var builder = new LogMessageBuilder(msg);
			builder
				.AddCustomData(nameof(executeResult.ExecutionId), executeResult.ExecutionId.ToString())
				.AddCustomData(nameof(executeResult.ExecuteStatus), executeResult.ExecuteStatus.ToString())
				.AddCustomData(nameof(newExecuteStatus), newExecuteStatus?.ToString())
				.AddCustomData(nameof(job.Name), job.Name)
				.LogCode(logCode, force: true);
		}

		foreach (var msg in result.WarningMessages)
		{
			var builder = new LogMessageBuilder(msg);
			builder
				.AddCustomData(nameof(executeResult.ExecutionId), executeResult.ExecutionId.ToString())
				.AddCustomData(nameof(executeResult.ExecuteStatus), executeResult.ExecuteStatus.ToString())
				.AddCustomData(nameof(newExecuteStatus), newExecuteStatus?.ToString())
				.AddCustomData(nameof(job.Name), job.Name)
				.LogCode(logCode, force: true);
		}

		foreach (var msg in result.ErrorMessages)
		{
			var builder = new ErrorMessageBuilder(msg);
			builder
				.AddCustomData(nameof(executeResult.ExecutionId), executeResult.ExecutionId.ToString())
				.AddCustomData(nameof(executeResult.ExecuteStatus), executeResult.ExecuteStatus.ToString())
				.AddCustomData(nameof(newExecuteStatus), newExecuteStatus?.ToString())
				.AddCustomData(nameof(job.Name), job.Name)
				.LogCode(logCode, force: true);
		}

		executeResult.SetStatus(newExecuteStatus);

		var msgs = new List<DbJobLog>();

		var messages = new List<ILogMessage>(result.ErrorMessages);
		messages.AddRange(result.WarningMessages);
		messages.AddRange(result.SuccessMessages);

		messages = messages.OrderBy(x => x.CreatedUtc).ToList();
		foreach (var message in messages)
		{
			switch (message.LogLevel)
			{
				case LogLevel.Trace:
					_logger.LogTraceMessage(message, true);
					break;
				case LogLevel.Debug:
					_logger.LogDebugMessage(message, true);
					break;
				case LogLevel.Information:
					_logger.LogInformationMessage(message, true);
					break;
				case LogLevel.Warning:
					_logger.LogWarningMessage(message, true);
					break;
				case LogLevel.Error:
					_logger.LogErrorMessage((message as IErrorMessage)!, true);
					break;
				case LogLevel.Critical:
					_logger.LogCriticalMessage((message as IErrorMessage)!, true);
					break;
				default:
					throw new NotSupportedException($"{nameof(message.LogLevel)} = {message.LogLevel}");
			}

			message.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization
			msgs.Add(DbJobLog.Create(job, executeResult, message, null, string.IsNullOrWhiteSpace(logCode) ? RESULT_LOG_CODE : logCode));
		}

		if (0 < msgs.Count)
		{
			try
			{
				await using var martenSession = _store.OpenSession();
				martenSession.Store((IEnumerable<DbJobLog>)msgs);
				await martenSession.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				_logger.LogErrorMessage(
					LogMessage.CreateErrorMessage(
						TraceInfo.Create(_applicationContext),
						x => x.ExceptionInfo(ex)
							.AddCustomData(nameof(executeResult.ExecutionId), executeResult.ExecutionId.ToString())
							.AddCustomData(nameof(executeResult.ExecuteStatus), executeResult.ExecuteStatus.ToString())
							.AddCustomData(nameof(newExecuteStatus), newExecuteStatus?.ToString())
							.LogCode(logCode, force: true)
							.AppendDetail($"{nameof(job.Name)} = {job.Name}")),
					true);
			}
		}
	}
}
