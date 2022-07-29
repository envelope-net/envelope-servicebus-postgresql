using Envelope.Converters;

namespace Envelope.ServiceBus.PostgreSql.Messages;

#nullable disable

public class DbJobData
{
	public Guid IdJobData { get; set; }
	public string JobName { get; set; }
}

public class DbJobData<TData> : DbJobData
{
	public TData Data { get; set; }

	public DbJobData<TData> Initialize(string jobName, TData data)
	{
		if (string.IsNullOrWhiteSpace(jobName))
			throw new ArgumentNullException(nameof(jobName));

		if (data == null)
			throw new ArgumentNullException(nameof(data));

		IdJobData = GuidConverter.ToGuid(jobName);
		JobName = jobName;
		Data = data;
		return this;
	}
}
