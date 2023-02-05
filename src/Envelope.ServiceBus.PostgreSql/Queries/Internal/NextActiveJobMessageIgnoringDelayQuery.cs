using Envelope.ServiceBus.Messages;
using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Queries.Internal;

public class NextActiveJobMessageIgnoringDelayQuery : ICompiledQuery<DbActiveJobMessage, DbActiveJobMessage?>
{
	private const int _idle = (int)JobMessageStatus.Idle;
	private const int _error = (int)JobMessageStatus.Error;
	private const int _suspended = (int)JobMessageStatus.Suspended;

	public int JobMessageTypeId { get; set; }

	public Expression<Func<IMartenQueryable<DbActiveJobMessage>, DbActiveJobMessage?>> QueryIs()
	{
		return q => q
			.Where(x =>
				x.JobMessageTypeId == JobMessageTypeId
				&& (x.Status == _idle || x.Status == _error || x.Status == _suspended))
			.OrderBy(x => x.CreatedUtc)
			.FirstOrDefault();
	}
}
