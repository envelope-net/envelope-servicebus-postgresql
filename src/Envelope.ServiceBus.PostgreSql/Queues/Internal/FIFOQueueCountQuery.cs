using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Queues.Internal;

public class FIFOQueueCountQuery : ICompiledQuery<DbQueuedMessage, int>
{
	public Expression<Func<IMartenQueryable<DbQueuedMessage>, int>> QueryIs()
	{
		return q => q.Count();
	}
}
