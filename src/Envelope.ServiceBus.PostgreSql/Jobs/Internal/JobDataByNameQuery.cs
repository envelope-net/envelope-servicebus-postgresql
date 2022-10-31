using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Exchange.Internal;

public class JobDataByNameQuery : ICompiledQuery<DbJobData, DbJobData?>
{
	public string? JobName { get; set; }

	public Expression<Func<IMartenQueryable<DbJobData>, DbJobData?>> QueryIs()
	{
		return q => q.FirstOrDefault(x => x.JobName == JobName);
	}
}
