namespace Envelope.ServiceBus.PostgreSql.Messages;

#nullable disable

public class DbExchangeArchivedMessage
{
	public Guid MessageId { get; set; }
	public ExchangeMessageDto ExchangeMessage { get; set; }
}
