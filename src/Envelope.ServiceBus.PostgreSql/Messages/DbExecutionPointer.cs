using Envelope.ServiceBus.Orchestrations.Execution;
using Envelope.ServiceBus.Orchestrations.Execution.Dto;

namespace Envelope.ServiceBus.PostgreSql.Messages;

#nullable disable

public class DbExecutionPointer
{
	public Guid IdExecutionPointer { get; set; }
	public Guid IdOrchestrationInstance { get; set; }
	public ExecutionPointerDto ExecutionPointer { get; set; }

	public DbExecutionPointer Initialize(ExecutionPointer executionPointer)
	{
		if (executionPointer == null)
			throw new ArgumentNullException(nameof(executionPointer));

		IdExecutionPointer = executionPointer.IdExecutionPointer;
		IdOrchestrationInstance = executionPointer.IdOrchestrationInstance;
		ExecutionPointer = new ExecutionPointerDto(executionPointer);
		return this;
	}
}
