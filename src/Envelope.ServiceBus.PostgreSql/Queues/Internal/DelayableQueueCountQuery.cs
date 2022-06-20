using Envelope.ServiceBus.Messages;
using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Queues.Internal;

public class DelayableQueueCountQuery : ICompiledQuery<DbQueuedMessage, int>
{
	public DateTime NowUtc { get; set; }

	public Expression<Func<IMartenQueryable<DbQueuedMessage>, int>> QueryIs()
	{
		return q => q
			.Count(x =>
				x.QueuedMessage.MessageStatus != MessageStatus.Completed
				&& x.QueuedMessage.MessageStatus != MessageStatus.Discarded
				&& x.QueuedMessage.MessageStatus != MessageStatus.Aborted
				&& x.QueuedMessage.MessageStatus != MessageStatus.Suspended
				&& (!x.QueuedMessage.DelayedToUtc.HasValue
					|| x.QueuedMessage.DelayedToUtc < NowUtc));
	}
}
