using Envelope.ServiceBus.Messages;
using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Exchange.Internal;

public class TryPeekFromDelayableQueueQuery : ICompiledQuery<DbExchangeMessage, DbExchangeMessage?>
{
	public DateTime NowUtc { get; set; }

	public Expression<Func<IMartenQueryable<DbExchangeMessage>, DbExchangeMessage?>> QueryIs()
	{
		return q => q
			.OrderBy(x => x.ExchangeMessage.PublishingTimeUtc)
			.FirstOrDefault(x =>
				x.ExchangeMessage.MessageStatus != MessageStatus.Completed
				&& x.ExchangeMessage.MessageStatus != MessageStatus.Discarded
				&& x.ExchangeMessage.MessageStatus != MessageStatus.Aborted
				&& x.ExchangeMessage.MessageStatus != MessageStatus.Suspended
				&& (!x.ExchangeMessage.DelayedToUtc.HasValue
					|| x.ExchangeMessage.DelayedToUtc < NowUtc));
	}
}
