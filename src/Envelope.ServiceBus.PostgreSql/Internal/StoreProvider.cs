using Envelope.ServiceBus.PostgreSql.Configuration;
using Envelope.ServiceBus.PostgreSql.Messages;
using Envelope.ServiceBus.PostgreSql.Serializers;
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

		if (_stores.ContainsKey(configuration.StoreKey))
			return;

		var jsonSerializer = new Marten.Services.JsonNetSerializer();

		jsonSerializer.Customize(serializer =>
		{
			serializer.Converters.Add(new HostInfoJsonConverter());
			serializer.Converters.Add(new EnvironmentInfoJsonConverter());
			serializer.Converters.Add(new LogMessageJsonConverter());
			serializer.Converters.Add(new TraceInfoJsonConverter());
			serializer.Converters.Add(new TraceFrameJsonConverter());

			//serializer.ReferenceLoopHandling = Newtonsoft.Json.ReferenceLoopHandling.Serialize;
			//serializer.PreserveReferencesHandling = Newtonsoft.Json.PreserveReferencesHandling.Objects;
			//serializer.TypeNameHandling = Newtonsoft.Json.TypeNameHandling.All;
		});

		var store = DocumentStore.For(options =>
		{
			options.Connection(configuration.ConnectionString);
			options.AutoCreateSchemaObjects = AutoCreate.None;
			options.DatabaseSchemaName = "esb";

			options.Serializer(jsonSerializer);

			//allow multi-tenant
			//configure.Policies.ForAllDocuments(x => x.TenancyStyle = TenancyStyle.Conjoined);

			options.Schema.For<DbHost>()
				.Identity(x => x.HostId)
				.DocumentAlias("host")
				.Duplicate(x => x.HostInfo.InstanceId,
					pgType: "uuid",
					notNull: true)
				.Duplicate(x => x.HostInfo.HostName,
					pgType: "varchar(255)",
					notNull: true)
				.Duplicate(x => x.HostStatus,
					pgType: "integer",
					notNull: true);

			options.Schema.For<DbHostLog>()
				.Identity(x => x.IdLogMessage)
				.DocumentAlias("host_log")
				.Duplicate(x => x.HostId,
					pgType: "uuid",
					notNull: true)
				.Duplicate(x => x.HostInstanceId,
					pgType: "uuid",
					notNull: true)
				.Duplicate(x => x.IdLogLevel,
					pgType: "integer",
					notNull: true)
				//.Duplicate(x => x.LogMessage.TraceInfo.CorrelationId!,
				//	pgType: "uuid",
				//	notNull: false)
				;

			options.Schema.For<DbHandlerLog>()
				.Identity(x => x.IdLogMessage)
				.DocumentAlias("handler_log")
				.Duplicate(x => x.IdLogLevel,
					pgType: "integer",
					notNull: true);
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
