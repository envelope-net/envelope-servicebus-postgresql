using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Exchange.Internal;

public class OrchestrationExecutionPointersQuery : ICompiledListQuery<DbExecutionPointer, DbExecutionPointer>
{
	public Guid IdOrchestrationInstance { get; set; }

	public Expression<Func<IMartenQueryable<DbExecutionPointer>, IEnumerable<DbExecutionPointer>>> QueryIs()
	{
		return q => q.Where(x => x.IdOrchestrationInstance == IdOrchestrationInstance);
	}
}
