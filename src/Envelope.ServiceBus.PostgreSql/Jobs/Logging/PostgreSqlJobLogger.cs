using Envelope.Logging;
using Envelope.Logging.Extensions;
using Envelope.ServiceBus.Jobs.Logging;
using Envelope.ServiceBus.PostgreSql.Internal;
using Envelope.ServiceBus.PostgreSql.Messages;
using Envelope.Trace;
using Envelope.Transactions;
using Marten;
using Microsoft.Extensions.Logging;

namespace Envelope.ServiceBus.PostgreSql.Jobs.Logging;

public class PostgreSqlJobLogger : IJobLogger
{
	private readonly DocumentStore _store;
	private readonly ILogger _logger;

	public PostgreSqlJobLogger(Guid storeKey, ILogger<PostgreSqlJobLogger> logger)
	{
		_logger = logger ?? throw new ArgumentNullException(nameof(logger));
		_store = StoreProvider.GetStore(storeKey);
	}

	private static Action<LogMessageBuilder> AppendToBuilder(
		Action<LogMessageBuilder> messageBuilder,
		string jobName,
		string? detail)
	{
		if (!string.IsNullOrWhiteSpace(jobName))
			messageBuilder += x => x.AddCustomData(nameof(jobName), jobName);

		if (!string.IsNullOrWhiteSpace(detail))
			messageBuilder +=
				x => x.AddCustomData(nameof(detail), detail);

		return messageBuilder;
	}

	private static Action<ErrorMessageBuilder> AppendToBuilder(
		Action<ErrorMessageBuilder> messageBuilder,
		string jobName,
		string? detail)
	{
		if (!string.IsNullOrWhiteSpace(jobName))
			messageBuilder += x => x.AddCustomData(nameof(jobName), jobName);

		if (!string.IsNullOrWhiteSpace(detail))
			messageBuilder +=
				x => x.AddCustomData(nameof(detail), detail);

		return messageBuilder;
	}

	public async Task<ILogMessage?> LogTraceAsync(
		ITraceInfo traceInfo,
		string jobName,
		Action<LogMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionContext? transactionContext = null,
		CancellationToken cancellationToken = default)
	{
		AppendToBuilder(messageBuilder, jobName, detail);
		var msg = _logger.PrepareTraceMessage(traceInfo, messageBuilder, true);
		if (msg != null)
		{
			_logger.LogTraceMessage(msg, true);

			var tmp = msg.Exception;
			msg.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization

			try
			{
				await using var martenSession = _store.OpenSession();
				martenSession.Store(new DbJobLog(jobName, msg));
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
		string jobName,
		Action<LogMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionContext? transactionContext = null,
		CancellationToken cancellationToken = default)
	{
		AppendToBuilder(messageBuilder, jobName, detail);
		var msg = _logger.PrepareDebugMessage(traceInfo, messageBuilder, true);
		if (msg != null)
		{
			_logger.LogDebugMessage(msg, true);

			var tmp = msg.Exception;
			msg.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization

			try
			{
				await using var martenSession = _store.OpenSession();
				martenSession.Store(new DbJobLog(jobName, msg));
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
		string jobName,
		Action<LogMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionContext? transactionContext = null,
		CancellationToken cancellationToken = default)
	{
		AppendToBuilder(messageBuilder, jobName, detail);
		var msg = _logger.PrepareInformationMessage(traceInfo, messageBuilder, true);
		if (msg != null)
		{
			_logger.LogInformationMessage(msg, true);

			var tmp = msg.Exception;
			msg.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization

			try
			{
				await using var martenSession = _store.OpenSession();
				martenSession.Store(new DbJobLog(jobName, msg));
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
		string jobName,
		Action<LogMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionContext? transactionContext = null,
		CancellationToken cancellationToken = default)
	{
		AppendToBuilder(messageBuilder, jobName, detail);
		var msg = _logger.PrepareWarningMessage(traceInfo, messageBuilder, true);
		if (msg != null)
		{
			_logger.LogWarningMessage(msg, true);

			var tmp = msg.Exception;
			msg.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization

			try
			{
				await using var martenSession = _store.OpenSession();
				martenSession.Store(new DbJobLog(jobName, msg));
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
		string jobName,
		Action<ErrorMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionContext? transactionContext = null,
		CancellationToken cancellationToken = default)
	{
		AppendToBuilder(messageBuilder, jobName, detail);
		var msg = _logger.PrepareErrorMessage(traceInfo, messageBuilder, false)!;
		_logger.LogErrorMessage(msg, true);

		var tmp = msg.Exception;
		msg.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization

		try
		{
			await using var martenSession = _store.OpenSession();
			martenSession.Store(new DbJobLog(jobName, msg));
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
		string jobName,
		Action<ErrorMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionContext? transactionContext = null,
		CancellationToken cancellationToken = default)
	{
		AppendToBuilder(messageBuilder, jobName, detail);
		var msg = _logger.PrepareCriticalMessage(traceInfo, messageBuilder, false)!;
		_logger.LogCriticalMessage(msg, true);

		var tmp = msg.Exception;
		msg.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization

		try
		{
			await using var martenSession = _store.OpenSession();
			martenSession.Store(new DbJobLog(jobName, msg));
			await martenSession.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			_logger.LogErrorMessage(LogMessage.CreateErrorMessage(TraceInfo.Create(traceInfo), x => x.ExceptionInfo(ex)), true);
		}

		msg.Exception = tmp;

		return msg;
	}
}
