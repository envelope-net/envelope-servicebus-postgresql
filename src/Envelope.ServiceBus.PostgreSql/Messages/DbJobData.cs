using Envelope.Converters;

namespace Envelope.ServiceBus.PostgreSql.Messages;

#nullable disable

public class DbJobData
{
	public Guid IdJobData { get; set; }
	public string JobName { get; set; }
	public object Data { get; set; }

	public DbJobData Initialize<TData>(string jobName, TData data)
	{
		if (string.IsNullOrWhiteSpace(jobName))
			throw new ArgumentNullException(nameof(jobName));

		IdJobData = GuidConverter.ToGuid(jobName);
		JobName = jobName;
		Data = data;
		return this;
	}
}
