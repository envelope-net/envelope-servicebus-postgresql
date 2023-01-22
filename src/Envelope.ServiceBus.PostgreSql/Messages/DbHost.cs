using Envelope.ServiceBus.Hosts;
using Envelope.ServiceBus.Queries;

namespace Envelope.ServiceBus.PostgreSql.Messages;

public class DbHost : IDbHost
{
	public Guid HostId { get; set; }
	public IHostInfo HostInfo { get; set; }
	public int HostStatus { get; set; }
	public DateTime LastUpdateUtc { get; set; }

	public static DbHost Create (IHostInfo hostInfo)
	{
		return new DbHost
		{
			HostInfo = hostInfo ?? throw new ArgumentNullException(nameof(hostInfo)),
			HostId = hostInfo.HostId,
			HostStatus = (int)hostInfo.HostStatus,
			LastUpdateUtc = DateTime.UtcNow
		};
	}

	public string ToJson()
		=> Newtonsoft.Json.JsonConvert.SerializeObject(this, Newtonsoft.Json.Formatting.Indented);

	public override string? ToString()
		=> HostInfo?.HostName ?? base.ToString();
}
