namespace Envelope.ServiceBus.PostgreSql.Messages;

#nullable disable

public class DbExchangeMessage
{
	public Guid MessageId { get; set; }
	public ExchangeMessageDto ExchangeMessage { get; set; }
}
