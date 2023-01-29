﻿using Envelope.ServiceBus.Jobs.Configuration;
using Envelope.ServiceBus.PostgreSql.Internal;
using Envelope.ServiceBus.PostgreSql.Jobs.Internal;
using Envelope.ServiceBus.PostgreSql.Jobs.Logging;
using Envelope.ServiceBus.PostgreSql.Queries.Internal;
using Envelope.ServiceBus.PostgreSql.Writers.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Envelope.ServiceBus.PostgreSql.Extensions;

public static class JobProviderConfigurationBuilderExtensions
{
	public static JobProviderConfigurationBuilder ConfigurePostgreSql(
		this JobProviderConfigurationBuilder builder,
		Guid storeKey)
	{
		if (builder == null)
			throw new ArgumentNullException(nameof(builder));

		if (storeKey == default)
			 storeKey = StoreProvider.DefaultStoreKey;

		builder
			.JobRepository(sp => new PostgreSqlJobRepository())
			.JobLogger(sp => new PostgreSqlJobLogger(
				storeKey,
				sp.GetRequiredService<IApplicationContext>(),
				sp.GetRequiredService<ILogger<PostgreSqlJobLogger>>()))
			.JobMessageReader(sp => new JobMessageReader(storeKey))
			.JobMessageWriter(sp => new JobMessageWriter(
				storeKey,
				//sp.GetRequiredService<IApplicationContext>(),
				sp.GetRequiredService<ILogger<JobMessageWriter>>()));

		return builder;
	}
}
