using Envelope.ServiceBus.Orchestrations.Model;
using Envelope.ServiceBus.PostgreSql.Configuration;
using Envelope.ServiceBus.PostgreSql.Messages;
using Marten;
using System.Collections.Concurrent;
using Weasel.Core;

namespace Envelope.ServiceBus.PostgreSql.Internal;

internal class StoreProvider
{
	internal static readonly Guid DefaultStoreKey = new("4FA4CD17-1957-4B78-BBB0-B1E464F4BF92");
	private static readonly ConcurrentDictionary<Guid, DocumentStore> _stores = new();

	public static void AddStore(IPostgreSqlStoreConfiguration configuration)
	{
		if (configuration == null)
			throw new ArgumentNullException(nameof(configuration));

		//var jsonSerializer = new Marten.Services.JsonNetSerializer
		//{
		//	EnumStorage = EnumStorage.AsString
		//};
		//jsonSerializer.Customize(serializer =>
		//{
		//	serializer.ReferenceLoopHandling = Newtonsoft.Json.ReferenceLoopHandling.Serialize;
		//	serializer.PreserveReferencesHandling = Newtonsoft.Json.PreserveReferencesHandling.Objects;
		//	serializer.TypeNameHandling = Newtonsoft.Json.TypeNameHandling.All;
		//});

		var store = DocumentStore.For(options =>
		{
			options.Connection(configuration.ConnectionString);
			options.AutoCreateSchemaObjects = AutoCreate.None;
			options.DatabaseSchemaName = "esb";

			//options.Serializer(jsonSerializer);

			//allow multi-tenant
			//configure.Policies.ForAllDocuments(x => x.TenancyStyle = TenancyStyle.Conjoined);

			options.Schema.For<DbExchangeMessage>()
				.Identity(x => x.MessageId)
				.UseOptimisticConcurrency(true);

			options.Schema.For<DbExchangeArchivedMessage>()
				.Identity(x => x.MessageId);

			options.Schema.For<DbQueuedMessage>()
				.Identity(x => x.MessageId)
				.UseOptimisticConcurrency(true);

			options.Schema.For<DbQueuedArchivedMessage>()
				.Identity(x => x.MessageId);

			options.Schema.For<DbHostLog>()
				.Identity(x => x.IdLogMessage);

			options.Schema.For<DbHandlerLog>()
				.Identity(x => x.IdLogMessage);

			options.Schema.For<DbOrchestrationInstance>()
				.Identity(x => x.IdOrchestrationInstance)
				.Duplicate(x => x.OrchestrationInstance.OrchestrationKey,
					pgType: "varchar(127)",
					notNull: true)
				.UseOptimisticConcurrency(true);

			options.Schema.For<DbExecutionPointer>()
				.Identity(x => x.IdExecutionPointer)
				.ForeignKey<DbOrchestrationInstance>(on => on.IdOrchestrationInstance)
				.UseOptimisticConcurrency(true);

			options.Schema.For<DbFinalizedBranches>()
				.Identity(x => x.IdOrchestrationInstance)
				.UseOptimisticConcurrency(true);

			options.Schema.For<DbOrchestrationEvent>()
				.Identity(x => x.MessageId)
				.UseOptimisticConcurrency(true);

			options.Schema.For<DbOrchestrationLog>()
				.Identity(x => x.IdLogMessage);
		});

		var SQL_SCRIPT = store.Storage.ToDatabaseScript();

		var added = _stores.TryAdd(configuration.StoreKey, store);
		if (!added)
			throw new InvalidOperationException($"No store added");
	}

	public static DocumentStore GetStore(Guid key)
	{
		_stores.TryGetValue(key, out var store);

		if (store == null)
			throw new InvalidOperationException($"No store found for key {key}");

		return store;
	}
}
