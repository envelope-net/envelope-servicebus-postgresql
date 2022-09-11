﻿using Envelope.ServiceBus.PostgreSql.Internal;
using Envelope.Transactions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Envelope.ServiceBus.Extensions;

public static partial class ServiceCollectionExtensions
{
	private static readonly string _postgreSqlTransactionDocumentSessionCacheType = typeof(PostgreSqlTransactionDocumentSessionCache).FullName!;

	public static IServiceCollection AddServiceBusPostgreSql(this IServiceCollection services, Guid storeKey)
	{
		services.TryAddTransient<ITransactionCoordinator, TransactionCoordinator>();
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
