using Envelope.ServiceBus.ErrorHandling;
using Envelope.ServiceBus.Messages;
using Envelope.ServiceBus.Queues;
using Envelope.Trace.Dto;
using System.Text;

namespace Envelope.ServiceBus.PostgreSql.Messages;

public class QueuedMessageDto
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





	public string QueueName { get; set; }

	public bool IsAsynchronousInvocation { get; set; }

	public string? RoutingKey { get; set; }

	public string SourceExchangeName { get; set; }

	public bool DisableFaultQueue { get; set; }

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
	public QueuedMessageDto()
	{
	}
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.

	public QueuedMessageDto(IQueuedMessage<IMessage> queuedMessage)
	{
		if (queuedMessage == null)
			throw new ArgumentNullException(nameof(queuedMessage));

		MessageId = queuedMessage.MessageId;
		Processed = queuedMessage.Processed;

		Message = queuedMessage.Message;

		ParentMessageId = queuedMessage.ParentMessageId;
		PublishingTimeUtc = queuedMessage.PublishingTimeUtc;
		PublisherId = queuedMessage.PublisherId;
		TraceInfo = new TraceInfoDto(queuedMessage.TraceInfo);
		Timeout = queuedMessage.Timeout;
		TimeToLiveUtc = queuedMessage.TimeToLiveUtc;
		IdSession = queuedMessage.IdSession;
		ContentType = queuedMessage.ContentType;
		ContentEncoding = queuedMessage.ContentEncoding;
		IsCompressedContent = queuedMessage.IsCompressedContent;
		IsEncryptedContent = queuedMessage.IsEncryptedContent;
		ContainsContent = queuedMessage.ContainsContent;
		HasSelfContent = queuedMessage.HasSelfContent;
		Priority = queuedMessage.Priority;
		Headers = queuedMessage.Headers;
		DisabledMessagePersistence = queuedMessage.DisabledMessagePersistence;
		MessageStatus = queuedMessage.MessageStatus;
		RetryCount = queuedMessage.RetryCount;
		ErrorHandling = queuedMessage.ErrorHandling;
		DelayedToUtc = queuedMessage.DelayedToUtc;

		QueueName = queuedMessage.QueueName;
		IsAsynchronousInvocation = queuedMessage.IsAsynchronousInvocation;
		RoutingKey = queuedMessage.RoutingKey;
		SourceExchangeName = queuedMessage.SourceExchangeName;
		DisableFaultQueue = queuedMessage.DisableFaultQueue;
	}

	public QueuedMessage<TMessage> ToQueuedMessage<TMessage>()
		where TMessage : class, IMessage
	{
		var queuedMessage = new QueuedMessage<TMessage>
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
			SourceExchangeName = SourceExchangeName,
			QueueName = QueueName,
			IsAsynchronousInvocation = IsAsynchronousInvocation,
			RoutingKey = RoutingKey,
			DisableFaultQueue = DisableFaultQueue
		};

		return queuedMessage;
	}

	public void Update(IMessageMetadataUpdate update)
	{
		if (update == null)
			throw new ArgumentNullException(nameof(update));

		if (MessageId != update.MessageId)
			throw new InvalidOperationException($"{nameof(MessageId)} != {nameof(update)}.{nameof(update.MessageId)} | {MessageId} != {update.MessageId}");

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
