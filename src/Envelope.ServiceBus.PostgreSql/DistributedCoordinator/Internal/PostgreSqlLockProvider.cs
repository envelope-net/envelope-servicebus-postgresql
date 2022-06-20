using Envelope.ServiceBus.DistributedCoordinator;
using Envelope.ServiceBus.PostgreSql.Internal;
using Envelope.Threading;
using Marten;

namespace Envelope.ServiceBus.PostgreSql.DistributedCoordinator.Internal;

internal class PostgreSqlLockProvider : IDistributedLockProvider
{
	private readonly AsyncLock _locker = new();
	private readonly DocumentStore _store;

	public PostgreSqlLockProvider(Guid storeKey)
	{
		_store = StoreProvider.GetStore(storeKey);
	}

	public async Task<LockResult> AcquireLockAsync(
		IDistributedLockKeyFactory distributedLockKeyProvider,
		string owner,
		DateTime expirationUtc,
		CancellationToken cancellationToken)
	{
		if (distributedLockKeyProvider == null)
			throw new ArgumentNullException(nameof(distributedLockKeyProvider));

		if (string.IsNullOrWhiteSpace(owner))
			throw new ArgumentNullException(nameof(owner));

		var key = distributedLockKeyProvider.CreateDistributedLockKey();

		using (await _locker.LockAsync().ConfigureAwait(false))
		{
			//if (_store.TryGetValue(key, out IDistributedLock distributedLock) && distributedLock.Owner != owner)
			//	return new LockResult(false, distributedLock.Owner);

			//_store.Set(key, new DistributedLock { ResourceType = distributedLockKeyProvider.DistributedLockResourceType, Key = key, Owner = owner, ExpireUtc = expirationUtc }, expirationUtc);
			return new LockResult(true, null);
		}
	}

	public async Task<LockResult> ReleaseLockAsync(IDistributedLockKeyFactory distributedLockKeyProvider, ISyncData syncData)
	{
		if (distributedLockKeyProvider == null)
			throw new ArgumentNullException(nameof(distributedLockKeyProvider));

		if (syncData == null)
			throw new ArgumentNullException(nameof(syncData));

		var key = distributedLockKeyProvider.CreateDistributedLockKey();

		using (await _locker.LockAsync().ConfigureAwait(false))
		{
			//if (_store.TryGetValue(key, out IDistributedLock distributedLock) && distributedLock.Owner != syncData.Owner)
			//	return new LockResult(false, distributedLock.Owner);

			//_store.Remove(key);
			return new LockResult(true, null);
		}
	}
}
