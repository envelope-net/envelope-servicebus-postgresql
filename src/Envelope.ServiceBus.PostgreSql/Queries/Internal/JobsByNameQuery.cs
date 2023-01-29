using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Events.CodeGeneration;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Exchange.Internal;

public class JobsByNameQuery : ICompiledListQuery<DbJob, DbJob>
{
	public string Name { get; set; }
	public string? HostName { get; set; }

	public int PageSize { get; set; } = 5;
	//[MartenIgnore] public int Page { private get; set; } = 1;
	//public int SkipCount => (Page - 1) * PageSize;

	public Expression<Func<IMartenQueryable<DbJob>, IEnumerable<DbJob>>> QueryIs()
	{
		return q => q
			.Where(x => x.Name == Name
				&& (HostName == null || x.HostName == HostName))
			.OrderByDescending(x => x.LastUpdateUtc)
			//.Skip(SkipCount)
			.Take(PageSize);
	}
}
