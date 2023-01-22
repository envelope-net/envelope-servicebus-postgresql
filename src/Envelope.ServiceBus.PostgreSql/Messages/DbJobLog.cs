using Envelope.Enums;
using Envelope.Logging;
using Envelope.ServiceBus.Jobs;
using Envelope.ServiceBus.Queries;
using Microsoft.Extensions.Logging;

namespace Envelope.ServiceBus.PostgreSql.Messages;

public class DbJobLog : IDbJobLog
{
	public Guid IdLogMessage { get; set; }
	public Guid JobInstanceId { get; set; }
	public string? Detail { get; set; }
	public Guid ExecutionId { get; set; }
	public string LogCode { get; set; }
	public ILogMessage LogMessage { get; set; }
	public int IdLogLevel { get; set; }
	public int Status { get; set; }
	public int ExecuteStatus { get; set; }
	public DateTime CreatedUtc { get; set; }

	public static DbJobLog Create(IJob job, JobExecuteResult executeResult, ILogMessage logMessage, string? detail, string logCode)
	{
		if (job == null)
			throw new ArgumentNullException(nameof(job));

		if (executeResult == null)
			throw new ArgumentNullException(nameof(executeResult));

		if (logMessage == null)
			throw new ArgumentNullException(nameof(logMessage));

		return new DbJobLog
		{
			ExecutionId = executeResult.ExecutionId,
			IdLogMessage = logMessage.IdLogMessage,
			JobInstanceId = job.JobInstanceId,
			Detail = detail,
			LogCode = logCode,
			LogMessage = logMessage,
			IdLogLevel = logMessage.IdLogLevel,
			Status = (int)job.Status,
			ExecuteStatus = (int)executeResult.ExecuteStatus,
			CreatedUtc = DateTime.UtcNow
		};
	}

	public string ToJson()
		=> Newtonsoft.Json.JsonConvert.SerializeObject(this, Newtonsoft.Json.Formatting.Indented);

	public override string ToString()
		=> $" {EnumHelper.ConvertIntToEnum<LogLevel>(IdLogLevel)} | {LogCode} | {EnumHelper.ConvertIntToEnum<JobExecuteStatus>(ExecuteStatus)}";
}
