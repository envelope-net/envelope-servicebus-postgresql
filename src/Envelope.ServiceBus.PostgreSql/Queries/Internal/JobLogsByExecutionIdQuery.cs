using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Exchange.Internal;

public class JobLogsByExecutionIdQuery : ICompiledListQuery<DbJobLog, DbJobLog>
{
	public Guid ExecutionId { get; set; }

	public Expression<Func<IMartenQueryable<DbJobLog>, IEnumerable<DbJobLog>>> QueryIs()
	{
		return q => q.Where(x => x.ExecutionId == ExecutionId);
	}
}
