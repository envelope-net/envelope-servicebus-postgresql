using Envelope.Logging;
using Envelope.Logging.Extensions;
using Envelope.ServiceBus.Hosts;
using Envelope.ServiceBus.Hosts.Logging;
using Envelope.ServiceBus.PostgreSql.Internal;
using Envelope.ServiceBus.PostgreSql.Messages;
using Envelope.Services;
using Envelope.Services.Extensions;
using Envelope.Trace;
using Envelope.Transactions;
using Marten;
using Microsoft.Extensions.Logging;

namespace Envelope.ServiceBus.PostgreSql.Hosts.Logging;

public class PostgreSqlHostLogger : IHostLogger
{
	private readonly DocumentStore _store;
	private readonly ILogger _logger;

	public PostgreSqlHostLogger(Guid storeKey, ILogger<PostgreSqlHostLogger> logger)
	{
		_store = StoreProvider.GetStore(storeKey);
		_logger = logger ?? throw new ArgumentNullException(nameof(logger));
	}

	private static Action<LogMessageBuilder> AppendToBuilder(
		Action<LogMessageBuilder> messageBuilder,
		IHostInfo hostInfo,
		HostStatus hostStatus,
		string? detail)
	{
		messageBuilder += x => x
			.AddCustomData(nameof(hostInfo.HostId), hostInfo.HostId.ToString())
			.AddCustomData(nameof(hostInfo.HostName), hostInfo.HostName.ToString())
			.AddCustomData(nameof(hostStatus), ((int)hostStatus).ToString());

		if (!string.IsNullOrWhiteSpace(detail))
			messageBuilder +=
				x => x.AddCustomData(nameof(detail), detail);

		return messageBuilder;
	}

	private static Action<ErrorMessageBuilder> AppendToBuilder(
		Action<ErrorMessageBuilder> messageBuilder,
		IHostInfo hostInfo,
		HostStatus hostStatus,
		string? detail)
	{
		messageBuilder += x => x
			.AddCustomData(nameof(hostInfo.HostId), hostInfo.HostId.ToString())
			.AddCustomData(nameof(hostInfo.HostName), hostInfo.HostName.ToString())
			.AddCustomData(nameof(hostStatus), ((int)hostStatus).ToString());

		if (!string.IsNullOrWhiteSpace(detail))
			messageBuilder +=
				x => x.AddCustomData(nameof(detail), detail);

		return messageBuilder;
	}

	public ILogMessage? LogTrace(
		ITraceInfo traceInfo,
		IHostInfo hostInfo,
		HostStatus hostStatus,
		Action<LogMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionManager? transactionManager = null)
	{
		AppendToBuilder(messageBuilder, hostInfo, hostStatus, detail);
		var msg = _logger.PrepareTraceMessage(traceInfo, messageBuilder, true);
		if (msg != null)
		{
			_logger.LogTraceMessage(msg, true);
			using var martenSession = _store.OpenSession();
			martenSession.Store(new DbHostLog(msg));
			martenSession.SaveChanges();
		}

		return msg;
	}

	public ILogMessage? LogDebug(
		ITraceInfo traceInfo,
		IHostInfo hostInfo,
		HostStatus hostStatus,
		Action<LogMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionManager? transactionManager = null)
	{
		AppendToBuilder(messageBuilder, hostInfo, hostStatus, detail);
		var msg = _logger.PrepareDebugMessage(traceInfo, messageBuilder, true);
		if (msg != null)
		{
			_logger.LogDebugMessage(msg, true);
			using var martenSession = _store.OpenSession();
			martenSession.Store(new DbHostLog(msg));
			martenSession.SaveChanges();
		}

		return msg;
	}

	public ILogMessage? LogInformation(
		ITraceInfo traceInfo,
		IHostInfo hostInfo,
		HostStatus hostStatus,
		Action<LogMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionManager? transactionManager = null)
	{
		AppendToBuilder(messageBuilder, hostInfo, hostStatus, detail);
		var msg = _logger.PrepareInformationMessage(traceInfo, messageBuilder, true);
		if (msg != null)
		{
			_logger.LogInformationMessage(msg, true);
			using var martenSession = _store.OpenSession();
			martenSession.Store(new DbHostLog(msg));
			martenSession.SaveChanges();
		}

		return msg;
	}

	public ILogMessage? LogWarning(
		ITraceInfo traceInfo,
		IHostInfo hostInfo,
		HostStatus hostStatus,
		Action<LogMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionManager? transactionManager = null)
	{
		AppendToBuilder(messageBuilder, hostInfo, hostStatus, detail);
		var msg = _logger.PrepareWarningMessage(traceInfo, messageBuilder, true);
		if (msg != null)
		{
			_logger.LogWarningMessage(msg, true);
			using var martenSession = _store.OpenSession();
			martenSession.Store(new DbHostLog(msg));
			martenSession.SaveChanges();
		}

		return msg;
	}

	public IErrorMessage LogError(
		ITraceInfo traceInfo,
		IHostInfo hostInfo,
		HostStatus hostStatus,
		Action<ErrorMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionManager? transactionManager = null)
	{
		AppendToBuilder(messageBuilder, hostInfo, hostStatus, detail);
		var msg = _logger.PrepareErrorMessage(traceInfo, messageBuilder, false)!;
		_logger.LogErrorMessage(msg, true);
		using var martenSession = _store.OpenSession();
		martenSession.Store(new DbHostLog(msg));
		martenSession.SaveChanges();

		return msg;
	}

	public IErrorMessage LogCritical(
		ITraceInfo traceInfo,
		IHostInfo hostInfo,
		HostStatus hostStatus,
		Action<ErrorMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionManager? transactionManager = null)
	{
		AppendToBuilder(messageBuilder, hostInfo, hostStatus, detail);
		var msg = _logger.PrepareCriticalMessage(traceInfo, messageBuilder, false)!;
		_logger.LogCriticalMessage(msg, true);
		using var martenSession = _store.OpenSession();
		martenSession.Store(new DbHostLog(msg));
		martenSession.SaveChanges();

		return msg;
	}

	public void LogResultErrorMessages(
		IResult result,
		ITransactionManager? transactionManager = null)
	{
		using var martenSession = _store.OpenSession();
		var msgs = new List<DbHostLog>();

		foreach (var errorMessage in result.ErrorMessages)
		{
			if (errorMessage.LogLevel == LogLevel.Error)
				_logger.LogErrorMessage(errorMessage, true);
			else if (errorMessage.LogLevel == LogLevel.Critical)
				_logger.LogCriticalMessage(errorMessage, true);
			else
				throw new NotSupportedException($"{nameof(errorMessage.LogLevel)} = {errorMessage.LogLevel}");

			msgs.Add(new DbHostLog(errorMessage));
		}

		martenSession.Store((IEnumerable<DbHostLog>)msgs);
		martenSession.SaveChanges();
	}

	public void LogResultAllMessages(
		IResult result,
		ITransactionManager? transactionManager = null)
	{
		using var martenSession = _store.OpenSession();
		var msgs = new List<DbHostLog>();

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

			msgs.Add(new DbHostLog(message));
		}

		martenSession.Store((IEnumerable<DbHostLog>)msgs);
		martenSession.SaveChanges();
	}

	public async Task<ILogMessage?> LogTraceAsync(
		ITraceInfo traceInfo,
		IHostInfo hostInfo,
		HostStatus hostStatus,
		Action<LogMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionManager? transactionManager = null,
		CancellationToken cancellationToken = default)
	{
		AppendToBuilder(messageBuilder, hostInfo, hostStatus, detail);
		var msg = _logger.PrepareTraceMessage(traceInfo, messageBuilder, true);
		if (msg != null)
		{
			_logger.LogTraceMessage(msg, true);
			using var martenSession = _store.OpenSession();
			martenSession.Store(new DbHostLog(msg));
			await martenSession.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
		}

		return msg;
	}

	public async Task<ILogMessage?> LogDebugAsync(
		ITraceInfo traceInfo,
		IHostInfo hostInfo,
		HostStatus hostStatus,
		Action<LogMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionManager? transactionManager = null,
		CancellationToken cancellationToken = default)
	{
		AppendToBuilder(messageBuilder, hostInfo, hostStatus, detail);
		var msg = _logger.PrepareDebugMessage(traceInfo, messageBuilder, true);
		if (msg != null)
		{
			_logger.LogDebugMessage(msg, true);
			using var martenSession = _store.OpenSession();
			martenSession.Store(new DbHostLog(msg));
			await martenSession.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
		}

		return msg;
	}

	public async Task<ILogMessage?> LogInformationAsync(
		ITraceInfo traceInfo,
		IHostInfo hostInfo,
		HostStatus hostStatus,
		Action<LogMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionManager? transactionManager = null,
		CancellationToken cancellationToken = default)
	{
		AppendToBuilder(messageBuilder, hostInfo, hostStatus, detail);
		var msg = _logger.PrepareInformationMessage(traceInfo, messageBuilder, true);
		if (msg != null)
		{
			_logger.LogInformationMessage(msg, true);
			using var martenSession = _store.OpenSession();
			martenSession.Store(new DbHostLog(msg));
			await martenSession.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
		}

		return msg;
	}

	public async Task<ILogMessage?> LogWarningAsync(
		ITraceInfo traceInfo,
		IHostInfo hostInfo,
		HostStatus hostStatus,
		Action<LogMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionManager? transactionManager = null,
		CancellationToken cancellationToken = default)
	{
		AppendToBuilder(messageBuilder, hostInfo, hostStatus, detail);
		var msg = _logger.PrepareWarningMessage(traceInfo, messageBuilder, true);
		if (msg != null)
		{
			_logger.LogWarningMessage(msg, true);
			using var martenSession = _store.OpenSession();
			martenSession.Store(new DbHostLog(msg));
			await martenSession.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
		}

		return msg;
	}

	public async Task<IErrorMessage> LogErrorAsync(
		ITraceInfo traceInfo,
		IHostInfo hostInfo,
		HostStatus hostStatus,
		Action<ErrorMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionManager? transactionManager = null,
		CancellationToken cancellationToken = default)
	{
		AppendToBuilder(messageBuilder, hostInfo, hostStatus, detail);
		var msg = _logger.PrepareErrorMessage(traceInfo, messageBuilder, false)!;
		_logger.LogErrorMessage(msg, true);
		using var martenSession = _store.OpenSession();
		martenSession.Store(new DbHostLog(msg));
		await martenSession.SaveChangesAsync(cancellationToken).ConfigureAwait(false);

		return msg;
	}

	public async Task<IErrorMessage> LogCriticalAsync(
		ITraceInfo traceInfo,
		IHostInfo hostInfo,
		HostStatus hostStatus,
		Action<ErrorMessageBuilder> messageBuilder,
		string? detail = null,
		ITransactionManager? transactionManager = null,
		CancellationToken cancellationToken = default)
	{
		AppendToBuilder(messageBuilder, hostInfo, hostStatus, detail);
		var msg = _logger.PrepareCriticalMessage(traceInfo, messageBuilder, false)!;
		_logger.LogCriticalMessage(msg, true);
		using var martenSession = _store.OpenSession();
		martenSession.Store(new DbHostLog(msg));
		await martenSession.SaveChangesAsync(cancellationToken).ConfigureAwait(false);

		return msg;
	}

	public async Task LogResultErrorMessagesAsync(
		IResult result,
		ITransactionManager? transactionManager = null,
		CancellationToken cancellationToken = default)
	{
		using var martenSession = _store.OpenSession();
		var msgs = new List<DbHostLog>();

		foreach (var errorMessage in result.ErrorMessages)
		{
			if (errorMessage.LogLevel == LogLevel.Error)
				_logger.LogErrorMessage(errorMessage, true);
			else if (errorMessage.LogLevel == LogLevel.Critical)
				_logger.LogCriticalMessage(errorMessage, true);
			else
				throw new NotSupportedException($"{nameof(errorMessage.LogLevel)} = {errorMessage.LogLevel}");

			msgs.Add(new DbHostLog(errorMessage));
		}

		if (0 < msgs.Count)
		{
			martenSession.Store((IEnumerable<DbHostLog>)msgs);
			await martenSession.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
		}
	}

	public async Task LogResultAllMessagesAsync(
		IResult result,
		ITransactionManager? transactionManager = null,
		CancellationToken cancellationToken = default)
	{
		using var martenSession = _store.OpenSession();
		var msgs = new List<DbHostLog>();

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

			msgs.Add(new DbHostLog(message));
		}

		if (0 < msgs.Count)
		{
			martenSession.Store((IEnumerable<DbHostLog>)msgs);
			await martenSession.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
		}
	}
}
