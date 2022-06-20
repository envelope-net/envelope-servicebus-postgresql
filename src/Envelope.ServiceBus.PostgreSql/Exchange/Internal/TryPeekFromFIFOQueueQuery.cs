using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Exchange.Internal;

public class TryPeekFromFIFOQueueQuery : ICompiledQuery<DbExchangeMessage, DbExchangeMessage?>
{
	public Expression<Func<IMartenQueryable<DbExchangeMessage>, DbExchangeMessage?>> QueryIs()
	{
		return q => q.OrderBy(x => x.ExchangeMessage.PublishingTimeUtc).FirstOrDefault();
	}
}
