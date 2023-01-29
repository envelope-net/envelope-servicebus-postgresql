﻿using Envelope.ServiceBus.Messages;
using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Queries.Internal;

public class NextActiveJobMessageIgnoringDelayQuery : ICompiledQuery<DbActiveJobMessage, DbActiveJobMessage?>
{
	private const int _idle = (int)JobMessageStatus.Idle;
	private const int _error = (int)JobMessageStatus.Error;

	public int JobMessageTypeId { get; set; }

	public Expression<Func<IMartenQueryable<DbActiveJobMessage>, DbActiveJobMessage?>> QueryIs()
	{
		return q => q
			.Where(x =>
				x.JobMessageTypeId == JobMessageTypeId
				&& (x.Status == _idle || x.Status == _error))
			.OrderBy(x => x.CreatedUtc)
			.FirstOrDefault();
	}
}
