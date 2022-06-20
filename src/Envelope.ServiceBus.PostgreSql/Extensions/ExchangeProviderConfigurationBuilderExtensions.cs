using Envelope.ServiceBus.Exchange;
using Envelope.ServiceBus.Exchange.Configuration;
using Envelope.ServiceBus.Exchange.Routing.Configuration;
using Envelope.ServiceBus.Messages;
using Envelope.ServiceBus.PostgreSql.Exchange.Internal;
using Envelope.ServiceBus.PostgreSql.Messages.Internal;

namespace Envelope.ServiceBus.PostgreSql.Extensions;

public static class ExchangeProviderConfigurationBuilderExtensions
{
	public static ExchangeProviderConfigurationBuilder RegisterPostgreSqlExchange<TMessage>(
		this ExchangeProviderConfigurationBuilder builder,
		Action<ExchangeRouterBuilder>? configureBindings = null,
		bool force = true)
		where TMessage : class, IMessage
		=> builder.RegisterExchange(
			typeof(TMessage).FullName!,
			sp =>
			{
				var exchangeConfiguration = ExchangeConfigurationBuilder<TMessage>
					.GetDefaultBuilder(builder.Internal().ServiceBusOptions, configureBindings)
					.FIFOQueue((sp, maxSize) => new DbExchangeMessageQueue<TMessage>(true))
					.DelayableQueue((sp, maxSize) => new DbExchangeMessageQueue<TMessage>(false))
					.MessageBodyProvider(sp => new PostgreSqlMessageBodyProvider())
					.Build();

				var context = new ExchangeContext<TMessage>(exchangeConfiguration);
				return new Exchange<TMessage>(context);
			},
			force);
}
