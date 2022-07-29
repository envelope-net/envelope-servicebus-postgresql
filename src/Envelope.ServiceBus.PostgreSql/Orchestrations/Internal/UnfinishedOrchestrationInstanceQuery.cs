using Envelope.ServiceBus.Orchestrations;
using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Exchange.Internal;

public class UnfinishedOrchestrationInstanceQuery : ICompiledListQuery<DbOrchestrationInstance, DbOrchestrationInstance>
{
	public Guid IdOrchestrationDefinition { get; set; }

	public Expression<Func<IMartenQueryable<DbOrchestrationInstance>, IEnumerable<DbOrchestrationInstance>>> QueryIs()
	{
		return q => q.Where(x => x.OrchestrationInstance.IdOrchestrationDefinition == IdOrchestrationDefinition
			&& (x.OrchestrationInstance.Status == OrchestrationStatus.Running
				|| x.OrchestrationInstance.Status == OrchestrationStatus.Executing));
	}
}
