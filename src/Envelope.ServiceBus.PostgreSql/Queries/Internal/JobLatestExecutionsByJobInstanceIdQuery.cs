using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Events.CodeGeneration;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Exchange.Internal;

public class JobLatestExecutionsByJobInstanceIdQuery : ICompiledListQuery<DbJobExecution, DbJobExecution>
{
	public Guid JobInstanceId { get; set; }

	public int PageSize { get; set; } = 20;
	//[MartenIgnore] public int Page { private get; set; } = 1;
	//public int SkipCount => (Page - 1) * PageSize;

	public Expression<Func<IMartenQueryable<DbJobExecution>, IEnumerable<DbJobExecution>>> QueryIs()
	{
		return q => q
			.Where(x => x.JobInstanceId == JobInstanceId)
			.OrderByDescending(x => x.StartedUtc)
			//.Skip(SkipCount)
			.Take(PageSize);
	}
}
