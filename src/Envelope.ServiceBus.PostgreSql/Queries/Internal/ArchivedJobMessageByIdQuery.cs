using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Queries.Internal;

public class ArchivedJobMessageByIdQuery : ICompiledQuery<DbArchivedJobMessage, DbArchivedJobMessage?>
{
	public Guid JobMessageId { get; set; }

	public Expression<Func<IMartenQueryable<DbArchivedJobMessage>, DbArchivedJobMessage?>> QueryIs()
	{
		return q => q
			.Where(x => x.Id == JobMessageId)
			.FirstOrDefault();
	}
}
