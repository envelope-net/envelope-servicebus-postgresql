using Envelope.ServiceBus.Orchestrations.Configuration;
using Envelope.ServiceBus.PostgreSql.DistributedCoordinator.Internal;
using Envelope.ServiceBus.PostgreSql.Internal;
using Envelope.ServiceBus.PostgreSql.Orchestrations.Internal;
using Envelope.ServiceBus.PostgreSql.Orchestrations.Logging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Envelope.ServiceBus.PostgreSql.Extensions;

public static class OrchestrationHostConfigurationBuilderExtensions
{
	public static OrchestrationHostConfigurationBuilder ConfigurePostgreSql(
		this OrchestrationHostConfigurationBuilder builder,
		Guid storeKey)
	{
		if (builder == null)
			throw new ArgumentNullException(nameof(builder));

		if (storeKey == default)
			 storeKey = StoreProvider.DefaultStoreKey;

		builder
			.TransactionManagerFactory(new PostgreSqlTransactionManagerFactory(storeKey))
			.TransactionContextFactory(ServiceBusConfigurationBuilderExtensions.CreateTransactionContextAsync)
			.OrchestrationRepositoryFactory((sp, registry) => new PostgreSqlOrchestrationRepository(registry))
			.DistributedLockProviderFactory(sp => new PostgreSqlLockProvider(storeKey))
			.OrchestrationLogger(sp => new PostgreSqlOrchestrationLogger(storeKey, sp.GetRequiredService<ILogger<PostgreSqlOrchestrationLogger>>()));

		return builder;
	}
}
