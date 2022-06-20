using Envelope.ServiceBus.ErrorHandling;
using Envelope.ServiceBus.Exchange;
using Envelope.ServiceBus.Messages;
using Envelope.Trace.Dto;
using System.Text;

namespace Envelope.ServiceBus.PostgreSql.Messages;

public class ExchangeMessageDto
{
	public Guid MessageId { get; set; }

	public bool Processed { get; set; }





	public IMessage? Message { get; set; }





	public Guid? ParentMessageId { get; set; }

	public DateTime PublishingTimeUtc { get; set; }

	public string PublisherId { get; set; }

	public TraceInfoDto TraceInfo { get; set; }

	public TimeSpan? Timeout { get; set; }

	public DateTime? TimeToLiveUtc { get; set; }

	public Guid? IdSession { get; set; }

	public string? ContentType { get; set; }

	public Encoding? ContentEncoding { get; set; }

	public bool ContainsContent { get; set; }

	public bool HasSelfContent { get; set; }

	public bool IsCompressedContent { get; set; }

	public bool IsEncryptedContent { get; set; }

	public int Priority { get; set; }

	public IEnumerable<KeyValuePair<string, object>>? Headers { get; set; }

	public bool DisabledMessagePersistence { get; set; }

	public MessageStatus MessageStatus { get; set; }

	public int RetryCount { get; set; }

	public IErrorHandlingController? ErrorHandling { get; set; }

	public DateTime? DelayedToUtc { get; set; }





	public string ExchangeName { get; set; }

	public bool IsAsynchronousInvocation { get; set; }

	public string? RoutingKey { get; set; }

	public string TargetQueueName { get; set; }

	public bool DisableFaultQueue { get; set; }

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
	public ExchangeMessageDto()
	{
	}
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.

	public ExchangeMessageDto(IExchangeMessage<IMessage> exchangeMessage)
	{
		if (exchangeMessage == null)
			throw new ArgumentNullException(nameof(exchangeMessage));

		MessageId = exchangeMessage.MessageId;
		Processed = exchangeMessage.Processed;

		Message = exchangeMessage.Message;

		ParentMessageId = exchangeMessage.ParentMessageId;
		PublishingTimeUtc = exchangeMessage.PublishingTimeUtc;
		PublisherId = exchangeMessage.PublisherId;
		TraceInfo = new TraceInfoDto(exchangeMessage.TraceInfo);
		Timeout = exchangeMessage.Timeout;
		TimeToLiveUtc = exchangeMessage.TimeToLiveUtc;
		IdSession = exchangeMessage.IdSession;
		ContentType = exchangeMessage.ContentType;
		ContentEncoding = exchangeMessage.ContentEncoding;
		IsCompressedContent = exchangeMessage.IsCompressedContent;
		IsEncryptedContent = exchangeMessage.IsEncryptedContent;
		ContainsContent = exchangeMessage.ContainsContent;
		HasSelfContent = exchangeMessage.HasSelfContent;
		Priority = exchangeMessage.Priority;
		Headers = exchangeMessage.Headers;
		DisabledMessagePersistence = exchangeMessage.DisabledMessagePersistence;
		MessageStatus = exchangeMessage.MessageStatus;
		RetryCount = exchangeMessage.RetryCount;
		ErrorHandling = exchangeMessage.ErrorHandling;
		DelayedToUtc = exchangeMessage.DelayedToUtc;

		ExchangeName = exchangeMessage.ExchangeName;
		IsAsynchronousInvocation = exchangeMessage.IsAsynchronousInvocation;
		RoutingKey = exchangeMessage.RoutingKey;
		TargetQueueName = exchangeMessage.TargetQueueName;
		DisableFaultQueue = exchangeMessage.DisableFaultQueue;
	}

	public ExchangeMessage<TMessage> ToExchangeMessage<TMessage>()
		where TMessage : class, IMessage
	{
		var exchangeMessage = new ExchangeMessage<TMessage>
		{
			MessageId = MessageId,
			ParentMessageId = ParentMessageId,
			PublishingTimeUtc = PublishingTimeUtc,
			PublisherId = PublisherId,
			TraceInfo = TraceInfo.ToTraceInfo(),
			Timeout = Timeout,
			IdSession = IdSession,
			ContentType = ContentType,
			ContentEncoding = ContentEncoding,
			IsCompressedContent = IsCompressedContent,
			IsEncryptedContent = IsEncryptedContent,
			ContainsContent = ContainsContent,
			HasSelfContent = HasSelfContent,
			Priority = Priority,
			Headers = Headers?.ToList(),
			DisabledMessagePersistence = DisabledMessagePersistence,
			MessageStatus = MessageStatus,
			RetryCount = RetryCount,
			ErrorHandling = ErrorHandling,
			DelayedToUtc = DelayedToUtc,

			Message = HasSelfContent ? (TMessage)Message! : null,

			Processed = Processed,
			ExchangeName = ExchangeName,
			TargetQueueName = TargetQueueName,
			IsAsynchronousInvocation = IsAsynchronousInvocation,
			RoutingKey = RoutingKey,
			DisableFaultQueue = DisableFaultQueue
		};

		return exchangeMessage;
	}

	public void Update(IMessageMetadataUpdate update)
	{
		if (update == null)
			throw new ArgumentNullException(nameof(update));

		if (MessageId != update.MessageId)
			throw new InvalidOperationException($"{nameof(MessageId)} == {nameof(update)}.{nameof(update.MessageId)}");

		Processed = update.Processed;
		MessageStatus = update.MessageStatus;
		RetryCount = update.RetryCount;
		DelayedToUtc = update.DelayedToUtc;
	}

	public void SetMessage(object message)
	{
		if (message == null)
			Message = null;

		if (message is not IMessage msg)
			throw new InvalidOperationException($"{nameof(message)} must be type of {typeof(IMessage).FullName}");

		Message = msg;
	}
}
