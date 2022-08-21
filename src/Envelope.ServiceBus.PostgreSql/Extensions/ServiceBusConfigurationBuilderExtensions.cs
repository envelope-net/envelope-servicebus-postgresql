using Envelope.ServiceBus.Configuration;
using Envelope.ServiceBus.Orchestrations.EventHandlers;
using Envelope.ServiceBus.Orchestrations.Model;
using Envelope.ServiceBus.PostgreSql.Configuration;
using Envelope.ServiceBus.PostgreSql.Exchange.Internal;
using Envelope.ServiceBus.PostgreSql.Hosts.Logging;
using Envelope.ServiceBus.PostgreSql.Internal;
using Envelope.ServiceBus.PostgreSql.MessageHandlers.Logging;
using Envelope.ServiceBus.PostgreSql.Messages.Internal;
using Envelope.ServiceBus.PostgreSql.Queues.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Envelope.ServiceBus.PostgreSql.Extensions;

public static class ServiceBusConfigurationBuilderExtensions
{
	public static ServiceBusConfigurationBuilder ConfigurePostgreSql(
		this ServiceBusConfigurationBuilder builder,
		Action<PostgreSqlStoreConfigurationBuilder> configure)
	{
		if (builder == null)
			throw new ArgumentNullException(nameof(builder));
		if (configure == null)
			throw new ArgumentNullException(nameof(configure));

		var storeBuilder = new PostgreSqlStoreConfigurationBuilder();
		configure.Invoke(storeBuilder);
		var postgreSqlStoreConfiguration = storeBuilder.Build();
		StoreProvider.AddStore(postgreSqlStoreConfiguration);
		var storeKey = postgreSqlStoreConfiguration.StoreKey;

		builder
			.OrchestrationEventsFaultQueue(sp => new PostgreSqlFaultQueue())
			.OrchestrationExchange(x => x
				.FIFOQueue((sp, maxSize) => new DbExchangeMessageQueue<OrchestrationEvent>(true))
				.DelayableQueue((sp, maxSize) => new DbExchangeMessageQueue<OrchestrationEvent>(false))
				.MessageBodyProvider(sp => new PostgreSqlMessageBodyProvider()))
			.OrchestrationQueue(x => x
				.FIFOQueue((sp, maxSize) => new DbMessageQueue<OrchestrationEvent>(true))
				.DelayableQueue((sp, maxSize) => new DbMessageQueue<OrchestrationEvent>(false))
				.MessageBodyProvider(sp => new PostgreSqlMessageBodyProvider())
				.MessageHandler((sp, options) => OrchestrationEventHandler.HandleMessageAsync))

			.HostLogger(sp => new PostgreSqlHostLogger(
				storeKey,
				sp.GetRequiredService<IApplicationContext>(),
				sp.GetRequiredService<ILogger<PostgreSqlHostLogger>>()))

			.HandlerLogger(sp => new PostgreSqlHandlerLogger(storeKey, sp.GetRequiredService<ILogger<PostgreSqlHandlerLogger>>()));

		return builder;
	}
}
