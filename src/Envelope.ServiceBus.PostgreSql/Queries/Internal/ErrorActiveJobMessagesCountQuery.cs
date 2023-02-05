﻿using Envelope.ServiceBus.Messages;
using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Queries.Internal;

public class ErrorActiveJobMessagesCountQuery : ICompiledQuery<DbActiveJobMessage, int>
{
	private const int _error = (int)JobMessageStatus.Error;

	public int JobMessageTypeId { get; set; }

	public Expression<Func<IMartenQueryable<DbActiveJobMessage>, int>> QueryIs()
	{
		return q => q.Where(x =>
			x.JobMessageTypeId == JobMessageTypeId
			&& x.Status == _error)
		.Count();
	}
}