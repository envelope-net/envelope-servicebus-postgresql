using Envelope.ServiceBus.Messages;
using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Queries.Internal;

public class ActiveJobMessagesToArchiveQuery : ICompiledListQuery<DbActiveJobMessage, DbActiveJobMessage>
{
	private const int _idle = (int)JobMessageStatus.Idle;
	private const int _error = (int)JobMessageStatus.Error;

	public DateTime LastUpdatedBeforeUtc { get; set; }

	public Expression<Func<IMartenQueryable<DbActiveJobMessage>, IEnumerable<DbActiveJobMessage>>> QueryIs()
	{
		return q => q.Where(x => x.LastUpdatedUtc < LastUpdatedBeforeUtc && x.Status != _idle && x.Status != _error);
	}
}
