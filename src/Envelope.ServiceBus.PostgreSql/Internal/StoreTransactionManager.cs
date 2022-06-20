using Envelope.Transactions;

namespace Envelope.ServiceBus.PostgreSql.Internal;

internal class StoreTransactionManager : TransactionManager, ITransactionManager, IDisposable, IAsyncDisposable
{
	private readonly Guid _storeKey;

	public StoreTransactionManager(Guid storeKey)
		: base()
	{
		_storeKey = storeKey;
	}

	public StoreTransactionManager(
		Guid storeKey,
		Action<ITransactionBehaviorObserverConnector>? configureBehavior,
		Action<ITransactionObserverConnector>? configure)
		: base(configureBehavior, configure)
	{
		_storeKey = storeKey;
	}

	public override ITransactionContext CreateTransactionContext()
	{
		var store = StoreProvider.GetStore(_storeKey);
		var transactionContext = new PostgreSqlTransactionContext(store, this);
		return transactionContext;
	}
}
