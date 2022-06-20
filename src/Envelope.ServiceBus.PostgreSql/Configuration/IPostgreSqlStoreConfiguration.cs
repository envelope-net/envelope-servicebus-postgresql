using Envelope.Validation;

namespace Envelope.ServiceBus.PostgreSql.Configuration;

public interface IPostgreSqlStoreConfiguration : IValidable
{
	Guid StoreKey { get; set; }
	string ConnectionString { get; set; }
}
