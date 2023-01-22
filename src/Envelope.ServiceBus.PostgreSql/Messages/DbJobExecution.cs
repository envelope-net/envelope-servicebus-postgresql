using Envelope.Enums;
using Envelope.ServiceBus.Jobs;
using Envelope.ServiceBus.Queries;

namespace Envelope.ServiceBus.PostgreSql.Messages;

public class DbJobExecution : IDbJobExecution
{
	public Guid ExecutionId { get; set; }
	public string JobName { get; set; }
	public Guid JobInstanceId { get; set; }
	public int ExecuteStatus { get; set; }
	public DateTime StartedUtc { get; set; }
	public DateTime? FinishedUtc { get; set; }

	public static DbJobExecution Started(IJob job, JobExecuteResult executeResult, DateTime startedUtc, bool finished = false)
	{
		if (job == null)
			throw new ArgumentNullException(nameof(job));

		if (executeResult == null)
			throw new ArgumentNullException(nameof(executeResult));

		return new DbJobExecution
		{
			ExecutionId = executeResult.ExecutionId,
			JobName = job.Name,
			JobInstanceId = job.JobInstanceId,
			ExecuteStatus = (int)executeResult.ExecuteStatus,
			StartedUtc = startedUtc,
			FinishedUtc = finished ? startedUtc : null
		};
	}

	public static DbJobExecution Finised(IJob job, JobExecuteResult executeResult, DateTime startedUtc)
	{
		if (job == null)
			throw new ArgumentNullException(nameof(job));

		if (executeResult == null)
			throw new ArgumentNullException(nameof(executeResult));

		return new DbJobExecution
		{
			ExecutionId = executeResult.ExecutionId,
			JobName = job.Name,
			JobInstanceId = job.JobInstanceId,
			ExecuteStatus = (int)executeResult.ExecuteStatus,
			StartedUtc = startedUtc,
			FinishedUtc = DateTime.UtcNow
		};
	}

	public string ToJson()
		=> Newtonsoft.Json.JsonConvert.SerializeObject(this, Newtonsoft.Json.Formatting.Indented);

	public override string ToString()
		=> $"{JobName} | {EnumHelper.ConvertIntToEnum<JobExecuteStatus>(ExecuteStatus)}";
}
