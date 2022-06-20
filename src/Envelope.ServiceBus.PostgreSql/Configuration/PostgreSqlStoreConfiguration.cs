using Envelope.ServiceBus.PostgreSql.Internal;
using Envelope.Text;
using System.Text;

namespace Envelope.ServiceBus.PostgreSql.Configuration;

public class PostgreSqlStoreConfiguration : IPostgreSqlStoreConfiguration
{
	public Guid StoreKey { get; set; } = StoreProvider.DefaultStoreKey;
	public string ConnectionString { get; set; }

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.

	public PostgreSqlStoreConfiguration()
	{
	}

#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.

	public StringBuilder? Validate(string? propertyPrefix = null, StringBuilder? parentErrorBuffer = null, Dictionary<string, object>? validationContext = null)
	{
		if (StoreKey == default)
		{
			if (parentErrorBuffer == null)
				parentErrorBuffer = new StringBuilder();

			parentErrorBuffer.AppendLine($"{StringHelper.ConcatIfNotNullOrEmpty(propertyPrefix, ".", nameof(StoreKey))} == null");
		}

		if (string.IsNullOrWhiteSpace(ConnectionString))
		{
			if (parentErrorBuffer == null)
				parentErrorBuffer = new StringBuilder();

			parentErrorBuffer.AppendLine($"{StringHelper.ConcatIfNotNullOrEmpty(propertyPrefix, ".", nameof(ConnectionString))} == null");
		}

		return parentErrorBuffer;
	}
}
