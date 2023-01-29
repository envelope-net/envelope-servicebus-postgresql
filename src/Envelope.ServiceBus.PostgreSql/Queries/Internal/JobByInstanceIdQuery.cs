using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Exchange.Internal;

public class JobByInstanceIdQuery : ICompiledQuery<DbJob, DbJob?>
{
	public Guid JobInstanceId { get; set; }

	public Expression<Func<IMartenQueryable<DbJob>, DbJob?>> QueryIs()
	{
		return q => q.Where(x => x.JobInstanceId == JobInstanceId).FirstOrDefault();
	}
}
