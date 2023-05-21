using Envelope.ServiceBus.PostgreSql.Configuration;
using Envelope.ServiceBus.PostgreSql.Internal;

namespace Envelope.ServiceBus.PostgreSql.Store;

public static class StoreFactory
{
	public static Guid CreateStore(Action<PostgreSqlStoreConfigurationBuilder> configure)
	{
		if (configure == null)
			throw new ArgumentNullException(nameof(configure));

		var storeBuilder = new PostgreSqlStoreConfigurationBuilder();
		configure.Invoke(storeBuilder);
		var postgreSqlStoreConfiguration = storeBuilder.Build();

		return CreateStore(postgreSqlStoreConfiguration);
	}

	public static Guid CreateStore(ConfigureServiceBusDb configure)
	{
		if (configure == null)
			throw new ArgumentNullException(nameof(configure));

		var storeBuilder = new PostgreSqlStoreConfigurationBuilder();
		configure.Invoke(storeBuilder);
		var postgreSqlStoreConfiguration = storeBuilder.Build();

		return CreateStore(postgreSqlStoreConfiguration);
	}

	private static Guid CreateStore(IPostgreSqlStoreConfiguration postgreSqlStoreConfiguration)
	{
		StoreProvider.AddStore(postgreSqlStoreConfiguration);
		var storeKey = postgreSqlStoreConfiguration.StoreKey;
		return storeKey;
	}
}
