﻿using Envelope.Logging;
using Envelope.Logging.Extensions;
using Envelope.ServiceBus.MessageHandlers.Logging;
using Envelope.ServiceBus.Messages;
using Envelope.ServiceBus.PostgreSql.Internal;
using Envelope.ServiceBus.PostgreSql.Messages;
using Envelope.Trace;
using Envelope.Transactions;
using Marten;
using Microsoft.Extensions.Logging;

namespace Envelope.ServiceBus.PostgreSql.MessageHandlers.Logging;

public class PostgreSqlHandlerLogger : IHandlerLogger
{
	private readonly DocumentStore _store;
	private readonly ILogger _logger;

	public PostgreSqlHandlerLogger(Guid storeKey, ILogger<PostgreSqlHandlerLogger> logger)
	{
		_store = StoreProvider.GetStore(storeKey);
		_logger = logger ?? throw new ArgumentNullException(nameof(logger));
	}

	private static Action<LogMessageBuilder> AppendToBuilder(
		Action<LogMessageBuilder> messageBuilder,
		string? detail)
	{
		if (!string.IsNullOrWhiteSpace(detail))
			messageBuilder +=
				x => x.AddCustomData(nameof(detail), detail);

		return messageBuilder;
	}

	private static Action<ErrorMessageBuilder> AppendToBuilder(
		Action<ErrorMessageBuilder> messageBuilder,
		string? detail)
	{
		if (!string.IsNullOrWhiteSpace(detail))
			messageBuilder +=
				x => x.AddCustomData(nameof(detail), detail);

		return messageBuilder;
	}

	public ILogMessage? LogTrace(
		ITraceInfo traceInfo,
		Action<LogMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionCoordinator? transactionCoordinator = null)
	{
		messageBuilder = AppendToBuilder(messageBuilder, detail);
		var msg = _logger.PrepareTraceMessage(traceInfo, messageBuilder, true);
		if (msg != null)
		{
			_logger.LogTraceMessage(msg, true);

			var tmp = msg.Exception;
			msg.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization

			try
			{
				using var martenSession = _store.OpenSession();
				martenSession.Store(new DbHandlerLog(msg));
				martenSession.SaveChanges();
			}
			catch (Exception ex)
			{
				_logger.LogErrorMessage(LogMessage.CreateErrorMessage(TraceInfo.Create(traceInfo), x => x.ExceptionInfo(ex)), true);
			}

			msg.Exception = tmp;
		}

		return msg;
	}

	public ILogMessage? LogDebug(
		ITraceInfo traceInfo,
		Action<LogMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionCoordinator? transactionCoordinator = null)
	{
		messageBuilder = AppendToBuilder(messageBuilder, detail);
		var msg = _logger.PrepareDebugMessage(traceInfo, messageBuilder, true);
		if (msg != null)
		{
			_logger.LogDebugMessage(msg, true);

			var tmp = msg.Exception;
			msg.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization

			try
			{
				using var martenSession = _store.OpenSession();
				martenSession.Store(new DbHandlerLog(msg));
				martenSession.SaveChanges();
			}
			catch (Exception ex)
			{
				_logger.LogErrorMessage(LogMessage.CreateErrorMessage(TraceInfo.Create(traceInfo), x => x.ExceptionInfo(ex)), true);
			}

			msg.Exception = tmp;
		}

		return msg;
	}

	public ILogMessage? LogInformation(
		ITraceInfo traceInfo,
		Action<LogMessageBuilder> messageBuilder,
		string? detail = null,
		bool force = false,
		ITransactionCoordinator? transactionCoordinator = null)
	{
		messageBuilder = AppendToBuilder(messageBuilder, detail);
		var msg = _logger.PrepareInformationMessage(traceInfo, messageBuilder, !force);
		if (msg != null)
		{
			_logger.LogInformationMessage(msg, true);

			var tmp = msg.Exception;
			msg.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization

			try
			{
				using var martenSession = _store.OpenSession();
				martenSession.Store(new DbHandlerLog(msg));
				martenSession.SaveChanges();
			}
			catch (Exception ex)
			{
				_logger.LogErrorMessage(LogMessage.CreateErrorMessage(TraceInfo.Create(traceInfo), x => x.ExceptionInfo(ex)), true);
			}

			msg.Exception = tmp;
		}

		return msg;
	}

	public ILogMessage? LogWarning(
		ITraceInfo traceInfo,
		Action<LogMessageBuilder> messageBuilder,
		string? detail = null,
		bool force = false,
		ITransactionCoordinator? transactionCoordinator = null)
	{
		messageBuilder = AppendToBuilder(messageBuilder, detail);
		var msg = _logger.PrepareWarningMessage(traceInfo, messageBuilder, !force);
		if (msg != null)
		{
			_logger.LogWarningMessage(msg, true);

			var tmp = msg.Exception;
			msg.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization

			try
			{
				using var martenSession = _store.OpenSession();
				martenSession.Store(new DbHandlerLog(msg));
				martenSession.SaveChanges();
			}
			catch (Exception ex)
			{
				_logger.LogErrorMessage(LogMessage.CreateErrorMessage(TraceInfo.Create(traceInfo), x => x.ExceptionInfo(ex)), true);
			}

			msg.Exception = tmp;
		}

		return msg;
	}

	public IErrorMessage LogError(
		ITraceInfo traceInfo,
		Action<ErrorMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionCoordinator? transactionCoordinator = null)
	{
		messageBuilder = AppendToBuilder(messageBuilder, detail);
		var msg = _logger.PrepareErrorMessage(traceInfo, messageBuilder, false)!;
		_logger.LogErrorMessage(msg, true);

		var tmp = msg.Exception;
		msg.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization

		try
		{
			using var martenSession = _store.OpenSession();
			martenSession.Store(new DbHandlerLog(msg));
			martenSession.SaveChanges();
		}
		catch (Exception ex)
		{
			_logger.LogErrorMessage(LogMessage.CreateErrorMessage(TraceInfo.Create(traceInfo), x => x.ExceptionInfo(ex)), true);
		}

		msg.Exception = tmp;

		return msg;
	}

	public IErrorMessage LogCritical(
		ITraceInfo traceInfo,
		Action<ErrorMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionCoordinator? transactionCoordinator = null)
	{
		messageBuilder = AppendToBuilder(messageBuilder, detail);
		var msg = _logger.PrepareCriticalMessage(traceInfo, messageBuilder, false)!;
		_logger.LogCriticalMessage(msg, true);

		var tmp = msg.Exception;
		msg.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization

		try
		{
			using var martenSession = _store.OpenSession();
			martenSession.Store(new DbHandlerLog(msg));
			martenSession.SaveChanges();
		}
		catch (Exception ex)
		{
			_logger.LogErrorMessage(LogMessage.CreateErrorMessage(TraceInfo.Create(traceInfo), x => x.ExceptionInfo(ex)), true);
		}

		msg.Exception = tmp;

		return msg;
	}

	public async Task<ILogMessage?> LogTraceAsync(
		ITraceInfo traceInfo,
		Action<LogMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionCoordinator? transactionCoordinator = null,
		CancellationToken cancellationToken = default)
	{
		messageBuilder = AppendToBuilder(messageBuilder, detail);
		var msg = _logger.PrepareTraceMessage(traceInfo, messageBuilder, true);
		if (msg != null)
		{
			_logger.LogTraceMessage(msg, true);

			var tmp = msg.Exception;
			msg.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization

			try
			{
				await using var martenSession = _store.OpenSession();
				martenSession.Store(new DbHandlerLog(msg));
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
		Action<LogMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionCoordinator? transactionCoordinator = null,
		CancellationToken cancellationToken = default)
	{
		messageBuilder = AppendToBuilder(messageBuilder, detail);
		var msg = _logger.PrepareDebugMessage(traceInfo, messageBuilder, true);
		if (msg != null)
		{
			_logger.LogDebugMessage(msg, true);

			var tmp = msg.Exception;
			msg.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization

			try
			{
				await using var martenSession = _store.OpenSession();
				martenSession.Store(new DbHandlerLog(msg));
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
		Action<LogMessageBuilder> messageBuilder,
		string? detail = null,
		bool force = false,
		ITransactionCoordinator? transactionCoordinator = null,
		CancellationToken cancellationToken = default)
	{
		messageBuilder = AppendToBuilder(messageBuilder, detail);
		var msg = _logger.PrepareInformationMessage(traceInfo, messageBuilder, !force);
		if (msg != null)
		{
			_logger.LogInformationMessage(msg, true);

			var tmp = msg.Exception;
			msg.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization

			try
			{
				await using var martenSession = _store.OpenSession();
				martenSession.Store(new DbHandlerLog(msg));
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
		Action<LogMessageBuilder> messageBuilder,
		string? detail = null,
		bool force = false,
		ITransactionCoordinator? transactionCoordinator = null,
		CancellationToken cancellationToken = default)
	{
		messageBuilder = AppendToBuilder(messageBuilder, detail);
		var msg = _logger.PrepareWarningMessage(traceInfo, messageBuilder, !force);
		if (msg != null)
		{
			_logger.LogWarningMessage(msg, true);

			var tmp = msg.Exception;
			msg.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization

			try
			{
				await using var martenSession = _store.OpenSession();
				martenSession.Store(new DbHandlerLog(msg));
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
		Action<ErrorMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionCoordinator? transactionCoordinator = null,
		CancellationToken cancellationToken = default)
	{
		messageBuilder = AppendToBuilder(messageBuilder, detail);
		var msg = _logger.PrepareErrorMessage(traceInfo, messageBuilder, false)!;
		_logger.LogErrorMessage(msg, true);

		var tmp = msg.Exception;
		msg.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization

		try
		{
			await using var martenSession = _store.OpenSession();
			martenSession.Store(new DbHandlerLog(msg));
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
		Action<ErrorMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionCoordinator? transactionCoordinator = null,
		CancellationToken cancellationToken = default)
	{
		messageBuilder = AppendToBuilder(messageBuilder, detail);
		var msg = _logger.PrepareCriticalMessage(traceInfo, messageBuilder, false)!;
		_logger.LogCriticalMessage(msg, true);

		var tmp = msg.Exception;
		msg.Exception = null; //marten's Newtonsoft Json serializer can failure on Exception serialization

		try
		{
			await using var martenSession = _store.OpenSession();
			martenSession.Store(new DbHandlerLog(msg));
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
