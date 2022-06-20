using Envelope.ServiceBus.Messages;
using Envelope.ServiceBus.PostgreSql.Internal;
using Envelope.ServiceBus.PostgreSql.Messages;
using Envelope.ServiceBus.Queues;
using Envelope.Services;
using Envelope.Trace;
using Envelope.Transactions;

namespace Envelope.ServiceBus.PostgreSql.Queues.Internal;

internal class DbMessageQueue<TMessage> : IQueue<IQueuedMessage<TMessage>>
	where TMessage : class, IMessage
{
	private readonly bool _isFIFO;
	private bool disposed;

	public int? MaxSize { get => null; set => _ = value; } //always null

	public DbMessageQueue(bool isFIFO)
	{
		_isFIFO = isFIFO;
	}

	public async Task<IResult<int>> GetCountAsync(ITraceInfo traceInfo, ITransactionContext transactionContext, CancellationToken cancellationToken = default)
	{
		traceInfo = TraceInfo.Create(traceInfo);
		var result = new ResultBuilder<int>();

		if (transactionContext == null)
			return result.WithArgumentNullException(traceInfo, nameof(transactionContext));

		var tc = ConvertTransactionContext(transactionContext);
		var martenSession = tc.CreateOrGetSession();
		int count;

		if (_isFIFO)
			count = await martenSession.QueryAsync(new FIFOQueueCountQuery(), cancellationToken).ConfigureAwait(false);
		else
			count = await martenSession.QueryAsync(new DelayableQueueCountQuery(), cancellationToken).ConfigureAwait(false);

		return result.WithData(count).Build();
	}

	public Task<IResult> EnqueueAsync(List<IQueuedMessage<TMessage>> queuedMessages, ITraceInfo traceInfo, ITransactionContext transactionContext, CancellationToken cancellationToken = default)
	{
		traceInfo = TraceInfo.Create(traceInfo);
		var result = new ResultBuilder();

		if (queuedMessages == null)
			throw new ArgumentNullException(nameof(queuedMessages));

		if (transactionContext == null)
			return Task.FromResult((IResult)result.WithArgumentNullException(traceInfo, nameof(transactionContext)));

		var tc = ConvertTransactionContext(transactionContext);
		var martenSession = tc.CreateOrGetSession();

		var dbQueuedMessages = queuedMessages.Select(x =>
		{
			var msg = new QueuedMessageDto(x);
			var wrapper = new DbQueuedMessage
			{
				MessageId = msg.MessageId,
				QueuedMessage = msg,
			};
			return wrapper;
		});
		martenSession.Store(dbQueuedMessages);

		return Task.FromResult((IResult)result.Build());
	}

	public async Task<IResult<IQueuedMessage<TMessage>?>> TryPeekAsync(ITraceInfo traceInfo, ITransactionContext transactionContext, CancellationToken cancellationToken = default)
	{
		traceInfo = TraceInfo.Create(traceInfo);
		var result = new ResultBuilder<IQueuedMessage<TMessage>?>();

		if (transactionContext == null)
			return result.WithArgumentNullException(traceInfo, nameof(transactionContext));

		var tc = ConvertTransactionContext(transactionContext);
		var martenSession = tc.CreateOrGetSession();

		DbQueuedMessage? existingDbQueuedMessage;

		if (_isFIFO)
			existingDbQueuedMessage = await martenSession.QueryAsync(new TryPeekFromFIFOQueueQuery(), cancellationToken).ConfigureAwait(false);
		else
			existingDbQueuedMessage = await martenSession.QueryAsync(new TryPeekFromDelayableQueueQuery(), cancellationToken).ConfigureAwait(false);

		if (existingDbQueuedMessage == null)
			return result.Build();

		var excahngeMessage = existingDbQueuedMessage.QueuedMessage?.ToQueuedMessage<TMessage>();
		return result.WithData(excahngeMessage).Build();
	}

	public async Task<IResult> TryRemoveAsync(IQueuedMessage<TMessage> queuedMessage, ITraceInfo traceInfo, ITransactionContext transactionContext, CancellationToken cancellationToken = default)
	{
		traceInfo = TraceInfo.Create(traceInfo);
		var result = new ResultBuilder();

		if (queuedMessage == null)
			return result.WithArgumentNullException(traceInfo, nameof(queuedMessage));

		if (transactionContext == null)
			return result.WithArgumentNullException(traceInfo, nameof(transactionContext));

		var tc = ConvertTransactionContext(transactionContext);
		var martenSession = tc.CreateOrGetSession();

		var existingDbQueuedMessage = await martenSession.LoadAsync<DbQueuedMessage>(queuedMessage.MessageId, cancellationToken).ConfigureAwait(false);
		if (existingDbQueuedMessage == null)
			return result.Build();

		martenSession.Delete(existingDbQueuedMessage);

		var dbQueuedArchivedMessage = new DbQueuedArchivedMessage
		{
			MessageId = existingDbQueuedMessage.MessageId,
			QueuedMessage = existingDbQueuedMessage.QueuedMessage,
		};

		martenSession.Store(dbQueuedArchivedMessage);

		return result.Build();
	}

	public async Task<IResult<QueueStatus>> UpdateAsync(IQueuedMessage<TMessage> queuedMessage, IMessageMetadataUpdate update, ITraceInfo traceInfo, ITransactionContext localTransactionContext, CancellationToken cancellationToken = default)
	{
		traceInfo = TraceInfo.Create(traceInfo);
		var result = new ResultBuilder<QueueStatus>();

		if (queuedMessage == null)
			return result.WithArgumentNullException(traceInfo, nameof(queuedMessage));

		if (update == null)
			return result.WithArgumentNullException(traceInfo, nameof(update));

		if (localTransactionContext == null)
			return result.WithArgumentNullException(traceInfo, nameof(localTransactionContext));

		var tc = ConvertTransactionContext(localTransactionContext);
		var martenSession = tc.CreateOrGetSession();

		var existingDbQueuedMessage = await martenSession.LoadAsync<DbQueuedMessage>(queuedMessage.MessageId, cancellationToken).ConfigureAwait(false);
		if (existingDbQueuedMessage == null)
			return result.WithInvalidOperationException(traceInfo, $"{nameof(existingDbQueuedMessage)} == null");

		var msg = new QueuedMessageDto(queuedMessage);
		msg.Update(update);
		existingDbQueuedMessage.QueuedMessage = msg;

		martenSession.Store(existingDbQueuedMessage);

		return 
			result
				.WithData((update.MessageStatus == MessageStatus.Suspended || update.MessageStatus == MessageStatus.Aborted)
					? QueueStatus.Suspended
					: QueueStatus.Running).Build();
	}

	private static PostgreSqlTransactionContext ConvertTransactionContext(ITransactionContext transactionContext)
	{
		if (transactionContext is not PostgreSqlTransactionContext tc)
			throw new InvalidOperationException($"{nameof(transactionContext)} must be type of {typeof(PostgreSqlTransactionContext).FullName}");

		return tc;
	}

	protected virtual void Dispose(bool disposing)
	{
		if (!disposed)
		{
			if (disposing)
			{
				//release managed resources
			}

			disposed = true;
		}
	}

	public void Dispose()
	{
		Dispose(disposing: true);
		GC.SuppressFinalize(this);
	}
}
