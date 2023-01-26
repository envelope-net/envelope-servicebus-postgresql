using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Exchange.Internal;

public class JobExecutionByExecutionIdQuery : ICompiledQuery<DbJobExecution, DbJobExecution?>
{
	public Guid ExecutionId { get; set; }

	public Expression<Func<IMartenQueryable<DbJobExecution>, DbJobExecution?>> QueryIs()
	{
		return q => q.Where(x => x.ExecutionId == ExecutionId).FirstOrDefault();
	}
}
