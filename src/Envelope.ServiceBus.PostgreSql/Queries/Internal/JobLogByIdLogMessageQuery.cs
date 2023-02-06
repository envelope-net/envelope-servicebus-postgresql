using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Exchange.Internal;

public class JobLogByIdLogMessageQuery : ICompiledQuery<DbJobLog, DbJobLog?>
{
	public Guid IdLogMessage { get; set; }

	public Expression<Func<IMartenQueryable<DbJobLog>, DbJobLog?>> QueryIs()
	{
		return q => q.Where(x => x.IdLogMessage == IdLogMessage).FirstOrDefault();
	}
}
