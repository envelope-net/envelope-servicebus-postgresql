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
		ITransactionController transactionController,
		CancellationToken cancellationToken = default)
	{
		if (string.IsNullOrWhiteSpace(jobName))
			throw new ArgumentNullException(nameof(jobName));

		if (transactionController == null)
			throw new ArgumentNullException(nameof(transactionController));

		var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
		var martenSession = tc.CreateOrGetSession();
		var dbJobData = await martenSession.QueryAsync(new JobDataByNameQuery<TData> { JobName = jobName }, cancellationToken).ConfigureAwait(false);

		if (dbJobData == null)
			return default;

		return dbJobData.Data;
	}

	public Task SaveDataAsync<TData>(
		string jobName,
		TData data,
		ITransactionController transactionController,
		CancellationToken cancellationToken = default)
	{
		if (string.IsNullOrWhiteSpace(jobName))
			throw new ArgumentNullException(nameof(jobName));

		if (transactionController == null)
			throw new ArgumentNullException(nameof(transactionController));

		var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
		var martenSession = tc.CreateOrGetSession();

		var dbJobData = new DbJobData<TData>().Initialize(jobName, data);

		if (data == null)
			martenSession.Delete(dbJobData.IdJobData);
		else
			martenSession.Store(dbJobData);

		return Task.CompletedTask;
	}
}
