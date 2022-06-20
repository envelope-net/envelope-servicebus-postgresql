using Envelope.Converters;
using Envelope.ServiceBus.Messages;
using Envelope.ServiceBus.Queues;
using Envelope.Services;
using Envelope.Trace;
using Envelope.Transactions;

namespace Envelope.ServiceBus.PostgreSql.Queues.Internal;

internal class PostgreSqlFaultQueue : IFaultQueue, IQueueInfo
{
	public Guid QueueId { get; }

	public string QueueName { get; }

	public bool IsFaultQueue => true;

	public bool IsPersistent => false;

	public int? MaxSize => null;

	public QueueType QueueType => QueueType.Parallel;

	public QueueStatus QueueStatus => QueueStatus.Running;

	public PostgreSqlFaultQueue()
	{
		QueueName = typeof(PostgreSqlFaultQueue).FullName!;
		QueueId = GuidConverter.ToGuid(QueueName);
	}

	public Task<IResult> EnqueueAsync(IMessage? message, IFaultQueueContext context, ITransactionContext transactionContext, CancellationToken cancellationToken)
		=> Task.FromResult((IResult)new ResultBuilder().Build());

	public Task<int> GetCountAsync(ITraceInfo traceInfo, ITransactionManagerFactory transactionManagerFactory, CancellationToken cancellationToken = default)
		=> Task.FromResult(0);
}
