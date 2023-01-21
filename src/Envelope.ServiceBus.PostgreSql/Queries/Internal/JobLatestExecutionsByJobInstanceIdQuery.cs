using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Exchange.Internal;

public class JobLatestExecutionsByJobInstanceIdQuery : ICompiledListQuery<DbJobExecution, DbJobExecution>
{
	public Guid JobInstanceId { get; set; }
	public int Count { get; set; } = 3;

	public Expression<Func<IMartenQueryable<DbJobExecution>, IEnumerable<DbJobExecution>>> QueryIs()
	{
		return q => q.Where(x => x.JobInstanceId == JobInstanceId).OrderByDescending(x => x.StartedUtc).Take(Count);
	}
}
