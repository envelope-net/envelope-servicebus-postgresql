using Envelope.ServiceBus.PostgreSql.Internal;
using Envelope.Validation;

namespace Envelope.ServiceBus.PostgreSql.Configuration;

public class PostgreSqlStoreConfiguration : IPostgreSqlStoreConfiguration, IValidable
{
	public Guid StoreKey { get; set; } = StoreProvider.DefaultStoreKey;
	public string ConnectionString { get; set; }

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.

	public PostgreSqlStoreConfiguration()
	{
	}

#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.

	public List<IValidationMessage>? Validate(
		string? propertyPrefix = null,
		ValidationBuilder? validationBuilder = null,
		Dictionary<string, object>? globalValidationContext = null,
		Dictionary<string, object>? customValidationContext = null)
	{
		validationBuilder ??= new ValidationBuilder();
		validationBuilder.SetValidationMessages(propertyPrefix, globalValidationContext)
			.IfDefault(StoreKey)
			.IfNullOrWhiteSpace(ConnectionString)
			;

		return validationBuilder.Build();
	}
}
