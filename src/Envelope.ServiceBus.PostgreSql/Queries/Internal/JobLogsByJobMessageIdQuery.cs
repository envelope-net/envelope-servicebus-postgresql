using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Exchange.Internal;

public class JobLogsByJobMessageIdQuery : ICompiledListQuery<DbJobLog, DbJobLog>
{
	public Guid JobMessageId { get; set; }

	public Expression<Func<IMartenQueryable<DbJobLog>, IEnumerable<DbJobLog>>> QueryIs()
	{
		return q => q.Where(x => x.JobMessageId == JobMessageId);
	}
}
