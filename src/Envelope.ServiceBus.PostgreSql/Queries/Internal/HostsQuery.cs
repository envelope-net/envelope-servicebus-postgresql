using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Exchange.Internal;

public class HostsQuery : ICompiledListQuery<DbHost, DbHost>
{
	public Expression<Func<IMartenQueryable<DbHost>, IEnumerable<DbHost>>> QueryIs()
	{
		return q => q.OrderBy(x => x.HostInfo.HostName);
	}
}
