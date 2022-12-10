using Envelope.Logging;

namespace Envelope.ServiceBus.PostgreSql.Messages;

public class DbHandlerLog
{
	public Guid IdLogMessage { get; set; }
	public ILogMessage LogMessage { get; set; }
	public int IdLogLevel { get; set; }

	public DbHandlerLog(ILogMessage logMessage)
	{
		if (logMessage == null)
			throw new ArgumentNullException(nameof(logMessage));

		IdLogMessage = logMessage.IdLogMessage;
		LogMessage = logMessage;
		IdLogLevel = logMessage.IdLogLevel;
	}
}
