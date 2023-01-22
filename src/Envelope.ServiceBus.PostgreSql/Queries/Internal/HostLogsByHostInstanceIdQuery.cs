using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Exchange.Internal;

public class HostLogsByHostInstanceIdQuery : ICompiledListQuery<DbHostLog, DbHostLog>
{
	public Guid HostInstanceId { get; set; }

	public Expression<Func<IMartenQueryable<DbHostLog>, IEnumerable<DbHostLog>>> QueryIs()
	{
		return q => q.Where(x => x.HostInstanceId == HostInstanceId);
	}
}
