using Envelope.Transactions;

namespace Envelope.ServiceBus.PostgreSql.Internal;

internal class PostgreSqlTransactionManagerFactory : ITransactionManagerFactory
{
	private readonly Guid _storeKey;

	public PostgreSqlTransactionManagerFactory(Guid storeKey)
	{
		_storeKey = storeKey;
	}

	public ITransactionManager Create()
		=> new StoreTransactionManager(_storeKey);

	public ITransactionManager Create(Action<ITransactionBehaviorObserverConnector>? configureBehavior, Action<ITransactionObserverConnector>? configure)
		=> new StoreTransactionManager(_storeKey, configureBehavior, configure);
}
