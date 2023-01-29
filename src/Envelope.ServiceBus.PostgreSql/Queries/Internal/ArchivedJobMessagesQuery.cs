using Envelope.ServiceBus.Messages;
using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Events.CodeGeneration;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Queries.Internal;

public class ArchivedJobMessagesQuery : ICompiledListQuery<DbArchivedJobMessage, DbArchivedJobMessage>
{
	private const int _deleted = (int)JobMessageStatus.Deleted;

	public int JobMessageTypeId { get; set; }
	public bool IncludeDeleted { get; set; }
	public int? Status { get; set; }

	public int PageSize { get; set; } = 20;
	[MartenIgnore] public int Page { private get; set; } = 1;
	public int SkipCount => (Page - 1) * PageSize;

	public Expression<Func<IMartenQueryable<DbArchivedJobMessage>, IEnumerable<DbArchivedJobMessage>>> QueryIs()
	{
		return q => q
			.Where(x =>
				x.JobMessageTypeId == JobMessageTypeId
				&& (IncludeDeleted || x.Status != _deleted)
				&& (!Status.HasValue || x.Status == Status))
			.OrderBy(x => x.CreatedUtc)
			.Skip(SkipCount)
			.Take(PageSize);
	}
}
