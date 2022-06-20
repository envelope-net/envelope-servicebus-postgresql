using Envelope.ServiceBus.Messages;
using Envelope.Services;
using Envelope.Trace;
using Envelope.Transactions;

namespace Envelope.ServiceBus.PostgreSql.Messages.Internal;

internal class PostgreSqlMessageBodyProvider : IMessageBodyProvider
{
	public Task<IResult> SaveToStorageAsync<TMessage>(List<IMessageMetadata> messagesMetadata, TMessage? message, ITraceInfo traceInfo, ITransactionContext transactionContext, CancellationToken cancellationToken)
		where TMessage : class, IMessage
	{
		var result = new ResultBuilder();

		if (messagesMetadata != null && message != null)
		{
			foreach (var metadata in messagesMetadata)
				metadata.HasSelfContent = false;

			//foreach (var metadata in messagesMetadata)
			//	_store.Set(metadata.MessageId, message, new MemoryCacheEntryOptions { SlidingExpiration = _slidingExpiration });
		}

		return Task.FromResult((IResult)result.Build());
	}

	public Task<IResult<Guid>> SaveReplyToStorageAsync<TResponse>(Guid messageId, TResponse? response, ITraceInfo traceInfo, ITransactionContext transactionContext, CancellationToken cancellationToken)
	{
		var result = new ResultBuilder<Guid>();

		if (response != null)
		{
			//_store.Set($"{messageId}:REPLY", response, new MemoryCacheEntryOptions { SlidingExpiration = _slidingExpiration });
		}

		return Task.FromResult(result.WithData(Guid.NewGuid()).Build());
	}

	public Task<IResult<TMessage?>> LoadFromStorageAsync<TMessage>(IMessageMetadata messageMetadata, ITraceInfo traceInfo, ITransactionContext transactionContext, CancellationToken cancellationToken)
		where TMessage : class, IMessage
	{
		var result = new ResultBuilder<TMessage?>();

		if (messageMetadata == null)
			throw new ArgumentNullException(nameof(messageMetadata));

		//if (_store.TryGetValue(messageMetadata.MessageId, out var message))
		//{
		//	if (message is TMessage tMessage)
		//		return Task.FromResult(result.WithData(tMessage).Build());
		//	else
		//		throw new InvalidOperationException($"Message with id {messageMetadata.MessageId} is not type of {typeof(TMessage).FullName}");
		//}

		return Task.FromResult(result.WithData(default).Build());
	}

	public bool AllowMessagePersistence(bool disabledMessagePersistence, IMessageMetadata message)
	{
		return !disabledMessagePersistence;
	}

	public bool AllowAnyMessagePersistence(bool disabledMessagePersistence, IEnumerable<IMessageMetadata> message)
	{
		return !disabledMessagePersistence;
	}
}
