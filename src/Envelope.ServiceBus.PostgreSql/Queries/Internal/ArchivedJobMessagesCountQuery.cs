using Envelope.ServiceBus.Messages;
using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Queries.Internal;

public class ArchivedJobMessagesCountQuery : ICompiledQuery<DbArchivedJobMessage, int>
{
	private const int _deleted = (int)JobMessageStatus.Deleted;

	public int JobMessageTypeId { get; set; }
	public bool IncludeDeleted { get; set; }
	public int? Status { get; set; }

	public Expression<Func<IMartenQueryable<DbArchivedJobMessage>, int>> QueryIs()
	{
		return q => q
			.Where(x =>
				x.JobMessageTypeId == JobMessageTypeId
				&& (IncludeDeleted || x.Status != _deleted)
				&& (!Status.HasValue || x.Status == Status))
			.Count();
	}
}
