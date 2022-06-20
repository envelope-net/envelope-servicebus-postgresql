using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Queues.Internal;

public class TryPeekFromFIFOQueueQuery : ICompiledQuery<DbQueuedMessage, DbQueuedMessage?>
{
	public Expression<Func<IMartenQueryable<DbQueuedMessage>, DbQueuedMessage?>> QueryIs()
	{
		return q => q.OrderBy(x => x.QueuedMessage.PublishingTimeUtc).FirstOrDefault();
	}
}
