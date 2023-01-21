using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Exchange.Internal;

public class JobsByHostInstanceIdQuery : ICompiledListQuery<DbJob, DbJob>
{
	public Guid HostInstanceId { get; set; }

	public Expression<Func<IMartenQueryable<DbJob>, IEnumerable<DbJob>>> QueryIs()
	{
		return q => q.Where(x => x.HostInstanceId == HostInstanceId);
	}
}
