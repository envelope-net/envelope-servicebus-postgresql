using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Exchange.Internal;

public class JobExecutionsByJobInstanceIdQuery : ICompiledListQuery<DbJobExecution, DbJobExecution>
{
	public Guid JobInstanceId { get; set; }
	public DateTime From { get; set; }
	public DateTime To { get; set; }

	public Expression<Func<IMartenQueryable<DbJobExecution>, IEnumerable<DbJobExecution>>> QueryIs()
	{
		return q => q.Where(x => x.JobInstanceId == JobInstanceId && From <= x.StartedUtc && x.StartedUtc <= To);
	}
}
