using Envelope.ServiceBus.Messages;
using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Queries.Internal;

public class AllActiveJobMessagesCountQuery : ICompiledQuery<DbActiveJobMessage, int>
{
	private const int _deleted = (int)JobMessageStatus.Deleted;

	public int JobMessageTypeId { get; set; }

	public Expression<Func<IMartenQueryable<DbActiveJobMessage>, int>> QueryIs()
	{
		return q => q.Where(x => x.JobMessageTypeId == JobMessageTypeId && x.Status != _deleted).Count();
	}
}
