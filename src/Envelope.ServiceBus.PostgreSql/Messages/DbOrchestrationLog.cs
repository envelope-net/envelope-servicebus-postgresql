using Envelope.Logging;

namespace Envelope.ServiceBus.PostgreSql.Messages;

public class DbOrchestrationLog
{
	public Guid IdLogMessage { get; set; }
	public ILogMessage LogMessage { get; set; }

	public DbOrchestrationLog(ILogMessage logMessage)
	{
		if (logMessage == null)
			throw new ArgumentNullException(nameof(logMessage));

		IdLogMessage = logMessage.IdLogMessage;
		LogMessage = logMessage;
	}
}
