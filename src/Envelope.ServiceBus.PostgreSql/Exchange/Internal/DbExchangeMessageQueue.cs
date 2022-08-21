using Envelope.ServiceBus.Exchange;
using Envelope.ServiceBus.Messages;
using Envelope.ServiceBus.PostgreSql.Internal;
using Envelope.ServiceBus.PostgreSql.Messages;
using Envelope.ServiceBus.Queues;
using Envelope.Services;
using Envelope.Trace;
using Envelope.Transactions;

namespace Envelope.ServiceBus.PostgreSql.Exchange.Internal;

internal class DbExchangeMessageQueue<TMessage> : IQueue<IExchangeMessage<TMessage>>
	where TMessage : class, IMessage
{
	private readonly bool _isFIFO;
	private bool _disposed;

	public int? MaxSize { get => null; set => _ = value; } //always null

	public DbExchangeMessageQueue(bool isFIFO)
	{
		_isFIFO = isFIFO;
	}

	public async Task<IResult<int>> GetCountAsync(ITraceInfo traceInfo, ITransactionController transactionController, CancellationToken cancellationToken = default)
	{
		traceInfo = TraceInfo.Create(traceInfo);
		var result = new ResultBuilder<int>();

		if (transactionController == null)
			return result.WithArgumentNullException(traceInfo, nameof(transactionController));

		var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
		var martenSession = tc.CreateOrGetSession();
		int count;

		if (_isFIFO)
			count = await martenSession.QueryAsync(new FIFOQueueCountQuery(), cancellationToken).ConfigureAwait(false);
		else
			count = await martenSession.QueryAsync(new DelayableQueueCountQuery(), cancellationToken).ConfigureAwait(false);

		return result.WithData(count).Build();
	}

	public Task<IResult> EnqueueAsync(List<IExchangeMessage<TMessage>> exchangeMessages, ITraceInfo traceInfo, ITransactionController transactionController, CancellationToken cancellationToken = default)
	{
		traceInfo = TraceInfo.Create(traceInfo);
		var result = new ResultBuilder();

		if (exchangeMessages == null)
			return Task.FromResult((IResult)result.WithArgumentNullException(traceInfo, nameof(exchangeMessages)));

		if (transactionController == null)
			return Task.FromResult((IResult)result.WithArgumentNullException(traceInfo, nameof(transactionController)));

		var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
		var martenSession = tc.CreateOrGetSession();

		var dbExchangeMessages = exchangeMessages.Select(x =>
		{
			var msg = new ExchangeMessageDto(x);
			var wrapper = new DbExchangeMessage
			{
				MessageId = msg.MessageId,
				ExchangeMessage = msg,
			};
			return wrapper;
		});
		martenSession.Store(dbExchangeMessages);

		return Task.FromResult((IResult)result.Build());
	}

	public async Task<IResult<IExchangeMessage<TMessage>?>> TryPeekAsync(ITraceInfo traceInfo, ITransactionController transactionController, CancellationToken cancellationToken = default)
	{
		traceInfo = TraceInfo.Create(traceInfo);
		var result = new ResultBuilder<IExchangeMessage<TMessage>?>();

		if (transactionController == null)
			return result.WithArgumentNullException(traceInfo, nameof(transactionController));

		var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
		var martenSession = tc.CreateOrGetSession();

		DbExchangeMessage? existingDbExchangeMessage;

		if (_isFIFO)
			existingDbExchangeMessage = await martenSession.QueryAsync(new TryPeekFromFIFOQueueQuery(), cancellationToken).ConfigureAwait(false);
		else
			existingDbExchangeMessage = await martenSession.QueryAsync(new TryPeekFromDelayableQueueQuery(), cancellationToken).ConfigureAwait(false);

		if (existingDbExchangeMessage == null)
			return result.Build();

		var excahngeMessage = existingDbExchangeMessage.ExchangeMessage?.ToExchangeMessage<TMessage>(traceInfo);
		return result.WithData(excahngeMessage).Build();
	}

	public async Task<IResult> TryRemoveAsync(IExchangeMessage<TMessage> exchangeMessage, ITraceInfo traceInfo, ITransactionController transactionController, CancellationToken cancellationToken = default)
	{
		traceInfo = TraceInfo.Create(traceInfo);
		var result = new ResultBuilder();

		if (exchangeMessage == null)
			return result.WithArgumentNullException(traceInfo, nameof(exchangeMessage));

		if (transactionController == null)
			return result.WithArgumentNullException(traceInfo, nameof(transactionController));

		var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
		var martenSession = tc.CreateOrGetSession();

		var existingDbExchangeMessage = await martenSession.LoadAsync<DbExchangeMessage>(exchangeMessage.MessageId, cancellationToken).ConfigureAwait(false);
		if (existingDbExchangeMessage == null)
			return result.Build();

		martenSession.Delete(existingDbExchangeMessage);

		var dbExchangeArchivedMessage = new DbExchangeArchivedMessage
		{
			MessageId = existingDbExchangeMessage.MessageId,
			ExchangeMessage = existingDbExchangeMessage.ExchangeMessage,
		};

		martenSession.Store(dbExchangeArchivedMessage);

		return result.Build();
	}

	public async Task<IResult<QueueStatus>> UpdateAsync(IExchangeMessage<TMessage> exchangeMessage, IMessageMetadataUpdate update, ITraceInfo traceInfo, ITransactionController localTransactionController, CancellationToken cancellationToken = default)
	{
		traceInfo = TraceInfo.Create(traceInfo);
		var result = new ResultBuilder<QueueStatus>();

		if (exchangeMessage == null)
			return result.WithArgumentNullException(traceInfo, nameof(exchangeMessage));

		if (update == null)
			return result.WithArgumentNullException(traceInfo, nameof(update));

		if (localTransactionController == null)
			return result.WithArgumentNullException(traceInfo, nameof(localTransactionController));

		var tc = localTransactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
		var martenSession = tc.CreateOrGetSession();

		var existingDbExchangeMessage = await martenSession.LoadAsync<DbExchangeMessage>(exchangeMessage.MessageId, cancellationToken).ConfigureAwait(false);
		if (existingDbExchangeMessage == null)
			return result.WithInvalidOperationException(traceInfo, $"{nameof(existingDbExchangeMessage)} == null");

		var msg = new ExchangeMessageDto(exchangeMessage);
		msg.Update(update);
		existingDbExchangeMessage.ExchangeMessage = msg;

		martenSession.Store(existingDbExchangeMessage);

		return 
			result
				.WithData((update.MessageStatus == MessageStatus.Suspended || update.MessageStatus == MessageStatus.Aborted)
					? QueueStatus.Suspended
					: QueueStatus.Running).Build();
	}

	protected virtual void Dispose(bool disposing)
	{
		if (_disposed)
			return;

		_disposed = true;

		if (disposing)
		{
			//release managed resources
		}
	}

	public void Dispose()
	{
		Dispose(disposing: true);
		GC.SuppressFinalize(this);
	}
}
