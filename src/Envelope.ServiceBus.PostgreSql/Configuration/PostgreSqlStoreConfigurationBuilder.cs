using Envelope.Exceptions;

namespace Envelope.ServiceBus.PostgreSql.Configuration;

public interface IPostgreSqlServiceBusConfigurationBuilder<TBuilder, TObject>
	where TBuilder : IPostgreSqlServiceBusConfigurationBuilder<TBuilder, TObject>
	where TObject : IPostgreSqlStoreConfiguration
{
	TBuilder Object(TObject postgreSqlServiceBusConfiguration);

	TObject Build(bool finalize = false);

	TBuilder StoreKey(Guid storeKey);

	TBuilder ConnectionString(string connectionString, bool force = true);
}

public abstract class PostgreSqlServiceBusConfigurationBuilderBase<TBuilder, TObject> : IPostgreSqlServiceBusConfigurationBuilder<TBuilder, TObject>
	where TBuilder : PostgreSqlServiceBusConfigurationBuilderBase<TBuilder, TObject>
	where TObject : IPostgreSqlStoreConfiguration
{
	private bool _finalized = false;
	protected readonly TBuilder _builder;
	protected TObject _postgreSqlServiceBusConfiguration;

	protected PostgreSqlServiceBusConfigurationBuilderBase(TObject postgreSqlServiceBusConfiguration)
	{
		_postgreSqlServiceBusConfiguration = postgreSqlServiceBusConfiguration;
		_builder = (TBuilder)this;
	}

	public virtual TBuilder Object(TObject postgreSqlServiceBusConfiguration)
	{
		_postgreSqlServiceBusConfiguration = postgreSqlServiceBusConfiguration;
		return _builder;
	}

	public TObject Build(bool finalize = false)
	{
		if (_finalized)
			throw new ConfigurationException("The builder was finalized");

		_finalized = finalize;

		if (_postgreSqlServiceBusConfiguration.StoreKey == Guid.Empty)
			_postgreSqlServiceBusConfiguration.StoreKey = Guid.NewGuid();

		var error = _postgreSqlServiceBusConfiguration.Validate(nameof(IPostgreSqlStoreConfiguration));
		if (0 < error?.Count)
			throw new ConfigurationException(error);

		return _postgreSqlServiceBusConfiguration;
	}

	public TBuilder StoreKey(Guid storeKey)
	{
		if (_finalized)
			throw new ConfigurationException("The builder was finalized");

		_postgreSqlServiceBusConfiguration.StoreKey = storeKey;
		return _builder;
	}

	public TBuilder ConnectionString(string connectionString, bool force = true)
	{
		if (_finalized)
			throw new ConfigurationException("The builder was finalized");

		if (force || string.IsNullOrWhiteSpace(_postgreSqlServiceBusConfiguration.ConnectionString))
			_postgreSqlServiceBusConfiguration.ConnectionString = connectionString;

		return _builder;
	}
}

public class PostgreSqlStoreConfigurationBuilder : PostgreSqlServiceBusConfigurationBuilderBase<PostgreSqlStoreConfigurationBuilder, IPostgreSqlStoreConfiguration>
{
	public PostgreSqlStoreConfigurationBuilder()
		: base(new PostgreSqlStoreConfiguration())
	{
	}

	public PostgreSqlStoreConfigurationBuilder(PostgreSqlStoreConfiguration postgreSqlServiceBusConfiguration)
		: base(postgreSqlServiceBusConfiguration)
	{
	}

	public static implicit operator PostgreSqlStoreConfiguration?(PostgreSqlStoreConfigurationBuilder builder)
	{
		if (builder == null)
			return null;

		return builder._postgreSqlServiceBusConfiguration as PostgreSqlStoreConfiguration;
	}

	public static implicit operator PostgreSqlStoreConfigurationBuilder?(PostgreSqlStoreConfiguration postgreSqlServiceBusConfiguration)
	{
		if (postgreSqlServiceBusConfiguration == null)
			return null;

		return new PostgreSqlStoreConfigurationBuilder(postgreSqlServiceBusConfiguration);
	}
}
