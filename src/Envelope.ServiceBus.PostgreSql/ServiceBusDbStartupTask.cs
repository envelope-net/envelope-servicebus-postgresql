using Envelope.DependencyInjection;
using Envelope.ServiceBus.PostgreSql.Configuration;
using Envelope.ServiceBus.PostgreSql.Store;
using Microsoft.Extensions.DependencyInjection;

namespace Envelope.ServiceBus.PostgreSql;

public delegate void ConfigureServiceBusDb(PostgreSqlStoreConfigurationBuilder builder);

public class ServiceBusDbStartupTask : IStartupTask
{
	public Task ExecuteAsync(IServiceProvider serviceProvider, CancellationToken cancellationToken = default)
	{
		var storeConfigure = serviceProvider.GetRequiredService<ConfigureServiceBusDb>();
		StoreFactory.CreateStore(storeConfigure);

		return Task.CompletedTask;
	}
}
