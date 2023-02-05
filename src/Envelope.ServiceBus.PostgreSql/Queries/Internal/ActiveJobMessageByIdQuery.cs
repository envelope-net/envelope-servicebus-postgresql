using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Queries.Internal;

public class ActiveJobMessageByIdQuery : ICompiledQuery<DbActiveJobMessage, DbActiveJobMessage?>
{
	public Guid JobMessageId { get; set; }

	public Expression<Func<IMartenQueryable<DbActiveJobMessage>, DbActiveJobMessage?>> QueryIs()
	{
		return q => q
			.Where(x => x.Id == JobMessageId)
			.FirstOrDefault();
	}
}
