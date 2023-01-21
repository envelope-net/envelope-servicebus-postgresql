using Envelope.Logging;
using Envelope.ServiceBus.Hosts;
using Envelope.ServiceBus.Queries;

namespace Envelope.ServiceBus.PostgreSql.Messages;

public class DbHostLog : IDbHostLog
{
	public Guid IdLogMessage { get; set; }
	public ILogMessage LogMessage { get; set; }
	public int IdLogLevel { get; set; }
	public Guid HostId { get; set; }
	public Guid HostInstanceId { get; set; }
	public int HostStatus { get; set; }
	public DateTime CreatedUtc { get; set; }

	public static DbHostLog Create(IHostInfo hostInfo, ILogMessage logMessage)
	{
		if (hostInfo == null)
			throw new ArgumentNullException(nameof(hostInfo));

		if (logMessage == null)
			throw new ArgumentNullException(nameof(logMessage));

		return new DbHostLog
		{
			IdLogMessage = logMessage.IdLogMessage,
			LogMessage = logMessage,
			IdLogLevel = logMessage.IdLogLevel,
			HostId = hostInfo.HostId,
			HostInstanceId = hostInfo.InstanceId,
			HostStatus = (int)hostInfo.HostStatus,
			CreatedUtc = DateTime.UtcNow
		};
	}

	public string ToJson()
		=> Newtonsoft.Json.JsonConvert.SerializeObject(this, Newtonsoft.Json.Formatting.Indented);
}
