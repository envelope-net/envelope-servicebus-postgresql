using Envelope.Logging;

namespace Envelope.ServiceBus.PostgreSql.Messages;

public class DbHostLog
{
	public Guid IdLogMessage { get; set; }
	public ILogMessage LogMessage { get; set; }

	public DbHostLog(ILogMessage logMessage)
	{
		if (logMessage == null)
			throw new ArgumentNullException(nameof(logMessage));

		IdLogMessage = logMessage.IdLogMessage;
		LogMessage = logMessage;
	}
}
