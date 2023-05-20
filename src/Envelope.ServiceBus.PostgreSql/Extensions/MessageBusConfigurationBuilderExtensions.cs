using Envelope.MessageBus.Configuration;
using Envelope.ServiceBus.PostgreSql.Configuration;
using Envelope.ServiceBus.PostgreSql.Hosts.Logging;
using Envelope.ServiceBus.PostgreSql.MessageHandlers.Logging;
using Envelope.ServiceBus.PostgreSql.Store;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Envelope.ServiceBus.PostgreSql.Extensions;

public static class MessageBusConfigurationBuilderExtensions
{
	public static MessageBusConfigurationBuilder ConfigurePostgreSql(
		this MessageBusConfigurationBuilder builder,
		Action<PostgreSqlStoreConfigurationBuilder> configure)
	{
		if (builder == null)
			throw new ArgumentNullException(nameof(builder));
		if (configure == null)
			throw new ArgumentNullException(nameof(configure));

		var storeKey = StoreFactory.CreateStore(configure);

		builder
			.HostLogger(sp => new PostgreSqlHostLogger(
				storeKey,
				sp.GetRequiredService<IApplicationContext>(),
				sp.GetRequiredService<ILogger<PostgreSqlHostLogger>>()))

			.HandlerLogger(sp => new PostgreSqlHandlerLogger(storeKey, sp.GetRequiredService<ILogger<PostgreSqlHandlerLogger>>()));

		return builder;
	}
}
