using Envelope.Logging;

namespace Envelope.ServiceBus.PostgreSql.Messages;

public class DbJobLog
{
	public Guid IdLogMessage { get; set; }
	public string JobName { get; set; }
	public string? Detail { get; set; }
	public ILogMessage LogMessage { get; set; }
	public int IdLogLevel { get; set; }

	public DbJobLog(string jobName, ILogMessage logMessage, string? detail)
	{
		if (string.IsNullOrWhiteSpace(jobName))
			throw new ArgumentNullException(nameof(jobName));

		if (logMessage == null)
			throw new ArgumentNullException(nameof(logMessage));

		IdLogMessage = logMessage.IdLogMessage;
		JobName = jobName;
		Detail = detail;
		LogMessage = logMessage;
		IdLogLevel = logMessage.IdLogLevel;
	}
}
