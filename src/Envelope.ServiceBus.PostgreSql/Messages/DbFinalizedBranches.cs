namespace Envelope.ServiceBus.PostgreSql.Messages;

public class DbFinalizedBranches
{
	public Guid IdOrchestrationInstance { get; set; }

	public List<Guid> StepIds { get; set; }

	public DbFinalizedBranches()
	{
		StepIds = new List<Guid>();
	}
}
