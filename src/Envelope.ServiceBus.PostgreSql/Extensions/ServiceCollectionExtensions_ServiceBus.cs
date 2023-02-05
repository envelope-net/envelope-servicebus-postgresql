using Envelope.ServiceBus.PostgreSql.Internal;
using Envelope.ServiceBus.PostgreSql.Queries.Internal;
using Envelope.ServiceBus.PostgreSql.Writers.Internal;
using Envelope.ServiceBus.Queries;
using Envelope.ServiceBus.Writers;
using Envelope.Transactions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace Envelope.ServiceBus.Extensions;

public static partial class ServiceCollectionExtensions
{
	private static readonly string _postgreSqlTransactionDocumentSessionCacheType = typeof(PostgreSqlTransactionDocumentSessionCache).FullName!;

	public static IServiceCollection AddServiceBusPostgreSql(this IServiceCollection services, Guid storeKey)
	{
		services.TryAddTransient<ITransactionCoordinator, TransactionCoordinator>();
		services.TryAddTransient<IServiceBusReader>(sp => new ServiceBusReader(storeKey));
		services.TryAddTransient<IJobMessagePublisher>(sp =>
		{
			var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
			return new JobMessageWriter(storeKey, loggerFactory.CreateLogger<JobMessageWriter>());
		});

		services.AddTransient<ITransactionCacheFactoryStore>(sp => new TransactionCacheFactoryStore(
			_postgreSqlTransactionDocumentSessionCacheType,
			serviceProvider =>
			{
				var store = StoreProvider.GetStore(storeKey);
				return new PostgreSqlTransactionDocumentSessionCache(store);
			}));

		return services;
	}
}
