namespace Envelope.ServiceBus.PostgreSql.Messages;

#nullable disable

public class DbQueuedArchivedMessage
{
	public Guid MessageId { get; set; }
	public QueuedMessageDto QueuedMessage { get; set; }
}
