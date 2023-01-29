﻿using Envelope.ServiceBus.Messages;
using Envelope.ServiceBus.PostgreSql.Messages;
using Marten.Linq;
using System.Linq.Expressions;

namespace Envelope.ServiceBus.PostgreSql.Queries.Internal;

public class ActiveJobMessagesQuery : ICompiledListQuery<DbActiveJobMessage, DbActiveJobMessage>
{
	private const int _deleted = (int)JobMessageStatus.Deleted;

	public int JobMessageTypeId { get; set; }

	public int PageSize { get; set; } = 20;
	//[MartenIgnore] public int Page { private get; set; } = 1;
	//public int SkipCount => (Page - 1) * PageSize;

	public Expression<Func<IMartenQueryable<DbActiveJobMessage>, IEnumerable<DbActiveJobMessage>>> QueryIs()
	{
		return q => q
			.Where(x =>
				x.JobMessageTypeId == JobMessageTypeId
				&& x.Status != _deleted)
			.OrderBy(x => x.CreatedUtc)
			//.Skip(SkipCount)
			.Take(PageSize);
	}
}
