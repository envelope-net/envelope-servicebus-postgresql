using Envelope.Logging;
using Envelope.Logging.Extensions;
using Envelope.ServiceBus.Orchestrations.Logging;
using Envelope.ServiceBus.PostgreSql.Internal;
using Envelope.ServiceBus.PostgreSql.Messages;
using Envelope.Trace;
using Envelope.Transactions;
using Marten;
using Microsoft.Extensions.Logging;

namespace Envelope.ServiceBus.PostgreSql.Orchestrations.Logging;

public class PostgreSqlOrchestrationLogger : IOrchestrationLogger
{
	private readonly DocumentStore _store;
	private readonly ILogger _logger;

	public PostgreSqlOrchestrationLogger(Guid storeKey, ILogger<PostgreSqlOrchestrationLogger> logger)
	{
		_logger = logger ?? throw new ArgumentNullException(nameof(logger));
		_store = StoreProvider.GetStore(storeKey);
	}

	private static Action<LogMessageBuilder> AppendToBuilder(
		Action<LogMessageBuilder> messageBuilder,
		Guid? idOrchestration,
		Guid? idStep,
		Guid? idExecutionPointer,
		string? detail)
	{
		if (idOrchestration.HasValue)
			messageBuilder += x => x.AddCustomData(nameof(idOrchestration), idOrchestration.Value.ToString());

		if (idStep.HasValue)
			messageBuilder += x => x.AddCustomData(nameof(idStep), idStep.Value.ToString());

		if (idExecutionPointer.HasValue)
			messageBuilder += x => x.AddCustomData(nameof(idExecutionPointer), idExecutionPointer.Value.ToString());

		if (!string.IsNullOrWhiteSpace(detail))
			messageBuilder +=
				x => x.AddCustomData(nameof(detail), detail);

		return messageBuilder;
	}

	private static Action<ErrorMessageBuilder> AppendToBuilder(
		Action<ErrorMessageBuilder> messageBuilder,
		Guid? idOrchestration,
		Guid? idStep,
		Guid? idExecutionPointer,
		string? detail)
	{
		if (idOrchestration.HasValue)
			messageBuilder += x => x.AddCustomData(nameof(idOrchestration), idOrchestration.Value.ToString());

		if (idStep.HasValue)
			messageBuilder += x => x.AddCustomData(nameof(idStep), idStep.Value.ToString());

		if (idExecutionPointer.HasValue)
			messageBuilder += x => x.AddCustomData(nameof(idExecutionPointer), idExecutionPointer.Value.ToString());

		if (!string.IsNullOrWhiteSpace(detail))
			messageBuilder +=
				x => x.AddCustomData(nameof(detail), detail);

		return messageBuilder;
	}

	public async Task<ILogMessage?> LogTraceAsync(
		ITraceInfo traceInfo,
		Guid? idOrchestration,
		Guid? idStep,
		Guid? idExecutionPointer,
		Action<LogMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionController? transactionController = null,
		CancellationToken cancellationToken = default)
	{
		AppendToBuilder(messageBuilder, idOrchestration, idStep, idExecutionPointer, detail);
		var msg = _logger.PrepareTraceMessage(traceInfo, messageBuilder, true);
		if (msg != null)
		{
			_logger.LogTraceMessage(msg, true);

			var tmp = msg.Exception;
			msg.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization

			try
			{
				await using var martenSession = _store.OpenSession();
				martenSession.Store(new DbOrchestrationLog(msg));
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
		Guid? idOrchestration,
		Guid? idStep,
		Guid? idExecutionPointer,
		Action<LogMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionController? transactionController = null,
		CancellationToken cancellationToken = default)
	{
		AppendToBuilder(messageBuilder, idOrchestration, idStep, idExecutionPointer, detail);
		var msg = _logger.PrepareDebugMessage(traceInfo, messageBuilder, true);
		if (msg != null)
		{
			_logger.LogDebugMessage(msg, true);

			var tmp = msg.Exception;
			msg.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization

			try
			{
				await using var martenSession = _store.OpenSession();
				martenSession.Store(new DbOrchestrationLog(msg));
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
		Guid? idOrchestration,
		Guid? idStep,
		Guid? idExecutionPointer,
		Action<LogMessageBuilder> messageBuilder,
		string? detail = null,
		bool force = false,
		ITransactionController? transactionController = null,
		CancellationToken cancellationToken = default)
	{
		AppendToBuilder(messageBuilder, idOrchestration, idStep, idExecutionPointer, detail);
		var msg = _logger.PrepareInformationMessage(traceInfo, messageBuilder, !force);
		if (msg != null)
		{
			_logger.LogInformationMessage(msg, true);

			var tmp = msg.Exception;
			msg.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization

			try
			{
				await using var martenSession = _store.OpenSession();
				martenSession.Store(new DbOrchestrationLog(msg));
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
		Guid? idOrchestration,
		Guid? idStep,
		Guid? idExecutionPointer,
		Action<LogMessageBuilder> messageBuilder,
		string? detail = null,
		bool force = false,
		ITransactionController? transactionController = null,
		CancellationToken cancellationToken = default)
	{
		AppendToBuilder(messageBuilder, idOrchestration, idStep, idExecutionPointer, detail);
		var msg = _logger.PrepareWarningMessage(traceInfo, messageBuilder, !force);
		if (msg != null)
		{
			_logger.LogWarningMessage(msg, true);

			var tmp = msg.Exception;
			msg.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization

			try
			{
				await using var martenSession = _store.OpenSession();
				martenSession.Store(new DbOrchestrationLog(msg));
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
		Guid? idOrchestration,
		Guid? idStep,
		Guid? idExecutionPointer,
		Action<ErrorMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionController? transactionController = null,
		CancellationToken cancellationToken = default)
	{
		AppendToBuilder(messageBuilder, idOrchestration, idStep, idExecutionPointer, detail);
		var msg = _logger.PrepareErrorMessage(traceInfo, messageBuilder, false)!;
		_logger.LogErrorMessage(msg, true);

		var tmp = msg.Exception;
		msg.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization

		try
		{
			await using var martenSession = _store.OpenSession();
			martenSession.Store(new DbOrchestrationLog(msg));
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
		Guid? idOrchestration,
		Guid? idStep,
		Guid? idExecutionPointer,
		Action<ErrorMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionController? transactionController = null,
		CancellationToken cancellationToken = default)
	{
		AppendToBuilder(messageBuilder, idOrchestration, idStep, idExecutionPointer, detail);
		var msg = _logger.PrepareCriticalMessage(traceInfo, messageBuilder, false)!;
		_logger.LogCriticalMessage(msg, true);

		var tmp = msg.Exception;
		msg.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization

		try
		{
			await using var martenSession = _store.OpenSession();
			martenSession.Store(new DbOrchestrationLog(msg));
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
