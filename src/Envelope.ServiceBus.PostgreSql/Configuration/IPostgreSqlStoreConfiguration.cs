using Envelope.Validation;

namespace Envelope.ServiceBus.PostgreSql.Configuration;

#if NET6_0_OR_GREATER
[Envelope.Serializer.JsonPolymorphicConverter]
#endif
public interface IPostgreSqlStoreConfiguration : IValidable
{
	Guid StoreKey { get; set; }
	string ConnectionString { get; set; }
}
