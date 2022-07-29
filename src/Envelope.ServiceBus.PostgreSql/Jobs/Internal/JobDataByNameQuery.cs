using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Exchange.Internal;

public class JobDataByNameQuery<TData> : ICompiledQuery<DbJobData<TData>, DbJobData<TData>?>
{
	public string? JobName { get; set; }

	public Expression<Func<IMartenQueryable<DbJobData<TData>>, DbJobData<TData>?>> QueryIs()
	{
		return q => q.FirstOrDefault(x => x.JobName == JobName);
	}
}
