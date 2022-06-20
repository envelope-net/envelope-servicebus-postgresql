using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Exchange.Internal;

public class FIFOQueueCountQuery : ICompiledQuery<DbExchangeMessage, int>
{
	public Expression<Func<IMartenQueryable<DbExchangeMessage>, int>> QueryIs()
	{
		return q => q.Count();
	}
}
