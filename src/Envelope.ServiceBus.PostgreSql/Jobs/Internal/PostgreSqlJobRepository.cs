using Envelope.ServiceBus.Jobs;
using Envelope.ServiceBus.PostgreSql.Exchange.Internal;
using Envelope.ServiceBus.PostgreSql.Internal;
using Envelope.ServiceBus.PostgreSql.Messages;
using Envelope.Transactions;

namespace Envelope.ServiceBus.PostgreSql.Jobs.Internal;

internal class PostgreSqlJobRepository : IJobRepository
{
	public async Task<TData?> LoadDataAsync<TData>(
		string jobName,
		ITransactionContext transactionContext,
		CancellationToken cancellationToken = default)
	{
		if (string.IsNullOrWhiteSpace(jobName))
			throw new ArgumentNullException(nameof(jobName));

		if (transactionContext == null)
			throw new ArgumentNullException(nameof(transactionContext));

		var tc = ConvertTransactionContext(transactionContext);
		var martenSession = tc.CreateOrGetSession();
		var dbJobData = await martenSession.QueryAsync(new JobDataByNameQuery<TData> { JobName = jobName }, cancellationToken).ConfigureAwait(false);

		if (dbJobData == null)
			return default;

		return dbJobData.Data;
	}

	public Task SaveDataAsync<TData>(
		string jobName,
		TData data,
		ITransactionContext transactionContext,
		CancellationToken cancellationToken = default)
	{
		if (string.IsNullOrWhiteSpace(jobName))
			throw new ArgumentNullException(nameof(jobName));

		if (transactionContext == null)
			throw new ArgumentNullException(nameof(transactionContext));

		var tc = ConvertTransactionContext(transactionContext);
		var martenSession = tc.CreateOrGetSession();

		var dbJobData = new DbJobData<TData>().Initialize(jobName, data);

		if (data == null)
			martenSession.Delete(dbJobData.IdJobData);
		else
			martenSession.Store(dbJobData);

		return Task.CompletedTask;
	}

	private static PostgreSqlTransactionContext ConvertTransactionContext(ITransactionContext transactionContext)
	{
		if (transactionContext is not PostgreSqlTransactionContext tc)
			throw new InvalidOperationException($"{nameof(transactionContext)} must be type of {typeof(PostgreSqlTransactionContext).FullName}");

		return tc;
	}
}
