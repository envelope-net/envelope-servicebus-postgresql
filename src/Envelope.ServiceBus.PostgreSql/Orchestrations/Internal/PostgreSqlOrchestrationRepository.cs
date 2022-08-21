using Envelope.Extensions;
using Envelope.ServiceBus.Hosts;
using Envelope.ServiceBus.Orchestrations;
using Envelope.ServiceBus.Orchestrations.Configuration;
using Envelope.ServiceBus.Orchestrations.Definition.Steps;
using Envelope.ServiceBus.Orchestrations.EventHandlers;
using Envelope.ServiceBus.Orchestrations.Execution;
using Envelope.ServiceBus.Orchestrations.Execution.Dto;
using Envelope.ServiceBus.Orchestrations.Model;
using Envelope.ServiceBus.PostgreSql.Exchange.Internal;
using Envelope.ServiceBus.PostgreSql.Internal;
using Envelope.ServiceBus.PostgreSql.Messages;
using Envelope.Services;
using Envelope.Trace;
using Envelope.Transactions;

namespace Envelope.ServiceBus.PostgreSql.Orchestrations.Internal;

internal class PostgreSqlOrchestrationRepository : IOrchestrationRepository, IOrchestrationEventQueue
{
	private readonly IOrchestrationRegistry _registry;

	public PostgreSqlOrchestrationRepository(IOrchestrationRegistry registry)
	{
		_registry = registry ?? throw new ArgumentNullException(nameof(registry));
	}

	public Task CreateNewOrchestrationAsync(IOrchestrationInstance orchestration, ITransactionController transactionController, CancellationToken cancellationToken = default)
	{
		if (orchestration == null)
			throw new ArgumentNullException(nameof(orchestration));

		if (transactionController == null)
			throw new ArgumentNullException(nameof(transactionController));

		var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
		var martenSession = tc.CreateOrGetSession();
		martenSession.Store(new DbOrchestrationInstance().Initialize(orchestration));

		return Task.CompletedTask;
	}

	public async Task<IOrchestrationInstance?> GetOrchestrationInstanceAsync(
		Guid idOrchestrationInstance,
		IServiceProvider serviceProvider,
		IHostInfo hostInfo,
		ITransactionController transactionController,
		CancellationToken cancellationToken)
	{
		if (serviceProvider == null)
			throw new ArgumentNullException(nameof(serviceProvider));

		if (hostInfo == null)
			throw new ArgumentNullException(nameof(hostInfo));

		if (transactionController == null)
			throw new ArgumentNullException(nameof(transactionController));

		var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
		var martenSession = tc.CreateOrGetSession();
		var dbOrchestrationInstance = await martenSession.LoadAsync<DbOrchestrationInstance>(idOrchestrationInstance, cancellationToken).ConfigureAwait(false);

		if (dbOrchestrationInstance == null)
			return null;

		var definition =
			_registry.GetDefinition(
				dbOrchestrationInstance.OrchestrationInstance.IdOrchestrationDefinition,
				dbOrchestrationInstance.OrchestrationInstance.Version);

		if (definition == null)
			throw new InvalidOperationException($"{nameof(definition)} == null");

		return dbOrchestrationInstance?.OrchestrationInstance.ToOrchestrationInstance(definition, serviceProvider, hostInfo);
	}

	public async Task<List<IOrchestrationInstance>> GetOrchestrationInstancesAsync(
		string orchestrationKey,
		IServiceProvider serviceProvider,
		IHostInfo hostInfo,
		ITransactionController transactionController,
		CancellationToken cancellationToken)
	{
		if (serviceProvider == null)
			throw new ArgumentNullException(nameof(serviceProvider));

		if (hostInfo == null)
			throw new ArgumentNullException(nameof(hostInfo));

		if (transactionController == null)
			throw new ArgumentNullException(nameof(transactionController));

		var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
		var martenSession = tc.CreateOrGetSession();
		var dbOrchestrationInstances = await martenSession.QueryAsync(new OrchestrationInstanceByKeyQuery { OrchestrationKey = orchestrationKey }, cancellationToken).ConfigureAwait(false);

		if (dbOrchestrationInstances == null || !dbOrchestrationInstances.Any())
			return new List<IOrchestrationInstance>();

		var instanceIds = dbOrchestrationInstances.Select(x => x.IdOrchestrationInstance).ToList().Distinct();

		var definitions =
			dbOrchestrationInstances
				.Select(x => 
					new {
						x.IdOrchestrationInstance,
						Definition = 
							_registry.GetDefinition(
								x.OrchestrationInstance.IdOrchestrationDefinition,
								x.OrchestrationInstance.Version)
					})
				.GroupBy(x => x.IdOrchestrationInstance)
				.ToDictionary(x => x.Key, x => x.First().Definition);

		foreach (var instanceId in instanceIds)
			if (!definitions.ContainsKey(instanceId))
				throw new InvalidOperationException($"{nameof(instanceId)} = {instanceId} has no definition");

		return dbOrchestrationInstances.Select(x => (IOrchestrationInstance)x.OrchestrationInstance.ToOrchestrationInstance(definitions[x.IdOrchestrationInstance]!, serviceProvider, hostInfo)).ToList();
	}

	public async Task<List<IOrchestrationInstance>> GetAllUnfinishedOrchestrationInstancesAsync(
		Guid idOrchestrationDefinition,
		IServiceProvider serviceProvider,
		IHostInfo hostInfo,
		ITransactionController transactionController,
		CancellationToken cancellationToken)
	{
		if (serviceProvider == null)
			throw new ArgumentNullException(nameof(serviceProvider));

		if (hostInfo == null)
			throw new ArgumentNullException(nameof(hostInfo));

		if (transactionController == null)
			throw new ArgumentNullException(nameof(transactionController));

		var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
		var martenSession = tc.CreateOrGetSession();
		
		var dbOrchestrationInstances = await martenSession.QueryAsync(new UnfinishedOrchestrationInstanceQuery { IdOrchestrationDefinition = idOrchestrationDefinition }, cancellationToken).ConfigureAwait(false);

		if (dbOrchestrationInstances == null || !dbOrchestrationInstances.Any())
			return new List<IOrchestrationInstance>();

		var instanceIds = dbOrchestrationInstances.Select(x => x.IdOrchestrationInstance).ToList().Distinct();

		var definitions =
			dbOrchestrationInstances
				.Select(x =>
					new {
						x.IdOrchestrationInstance,
						Definition =
							_registry.GetDefinition(
								x.OrchestrationInstance.IdOrchestrationDefinition,
								x.OrchestrationInstance.Version)
					})
				.GroupBy(x => x.IdOrchestrationInstance)
				.ToDictionary(x => x.Key, x => x.First().Definition);

		foreach (var instanceId in instanceIds)
			if (!definitions.ContainsKey(instanceId))
				throw new InvalidOperationException($"{nameof(instanceId)} = {instanceId} has no definition");

		return dbOrchestrationInstances.Select(x => (IOrchestrationInstance)x.OrchestrationInstance.ToOrchestrationInstance(definitions[x.IdOrchestrationInstance]!, serviceProvider, hostInfo)).ToList();
	}

	public async Task<bool?> IsCompletedOrchestrationAsync(Guid idOrchestrationInstance, ITransactionController transactionController, CancellationToken cancellationToken = default)
	{
		if (transactionController == null)
			throw new ArgumentNullException(nameof(transactionController));

		var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
		var martenSession = tc.CreateOrGetSession();
		var dbOrchestrationInstance = await martenSession.LoadAsync<DbOrchestrationInstance>(idOrchestrationInstance, cancellationToken).ConfigureAwait(false);

		if (dbOrchestrationInstance == null)
			return null;

		var dbExecutionPointers = await martenSession.QueryAsync(new OrchestrationExecutionPointersQuery { IdOrchestrationInstance = dbOrchestrationInstance.IdOrchestrationInstance }, cancellationToken).ConfigureAwait(false);

		return dbExecutionPointers?.All(x => x.ExecutionPointer.Status == PointerStatus.Completed) ?? false;
	}

	public Task AddExecutionPointerAsync(ExecutionPointer executionPointer, ITransactionController transactionController)
	{
		if (executionPointer == null)
			throw new ArgumentNullException(nameof(executionPointer));

		if (transactionController == null)
			throw new ArgumentNullException(nameof(transactionController));

		var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
		var martenSession = tc.CreateOrGetSession();
		martenSession.Store(new DbExecutionPointer().Initialize(executionPointer));
		return Task.CompletedTask;
	}

	public Task AddNestedExecutionPointerAsync(ExecutionPointer executionPointer, ExecutionPointer parentExecutionPointer, ITransactionController transactionController)
	{
		if (executionPointer == null)
			throw new ArgumentNullException(nameof(executionPointer));

		if (transactionController == null)
			throw new ArgumentNullException(nameof(transactionController));

		var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
		var martenSession = tc.CreateOrGetSession();
		martenSession.Store(new DbExecutionPointer().Initialize(executionPointer));
		return Task.CompletedTask;
	}

	public async Task<List<ExecutionPointer>> GetOrchestrationExecutionPointersAsync(Guid idOrchestrationInstance, ITransactionController transactionController)
	{
		if (transactionController == null)
			throw new ArgumentNullException(nameof(transactionController));

		var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
		var martenSession = tc.CreateOrGetSession();
		var dbExecutionPointers = await martenSession.QueryAsync(new OrchestrationExecutionPointersQuery { IdOrchestrationInstance = idOrchestrationInstance}).ConfigureAwait(false);

		if (dbExecutionPointers == null || !dbExecutionPointers.Any())
			return new List<ExecutionPointer>();

		var first = dbExecutionPointers.First();

		var definition =
			_registry.GetDefinition(
				first.ExecutionPointer.IdOrchestrationDefinition,
				first.ExecutionPointer.OrchestrationInstanceVersion);

		if (definition == null)
			throw new InvalidOperationException($"{nameof(definition)} == null");

		return dbExecutionPointers.Select(x =>
			x.ExecutionPointer.ToExecutionPointer(
				definition.Steps?.FirstOrDefault(s => s.IdStep == x.ExecutionPointer.IdStep)
					?? throw new InvalidOperationException($"step == null")))
			.ToList();
	}

	public async Task<ExecutionPointer?> GetStepExecutionPointerAsync(Guid idOrchestrationInstance, Guid idStep, ITransactionController transactionController)
	{
		if (transactionController == null)
			throw new ArgumentNullException(nameof(transactionController));

		var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
		var martenSession = tc.CreateOrGetSession();
		var dbExecutionPointer = await martenSession.QueryAsync(new ExecutionPointerByIdStepQuery { IdOrchestrationInstance = idOrchestrationInstance, IdStep = idStep }).ConfigureAwait(false);

		if (dbExecutionPointer == null)
			return null;

		var definition =
			_registry.GetDefinition(
				dbExecutionPointer.ExecutionPointer.IdOrchestrationDefinition,
				dbExecutionPointer.ExecutionPointer.OrchestrationInstanceVersion);

		if (definition == null)
			throw new InvalidOperationException($"{nameof(definition)} == null");

		var step = definition.Steps?.FirstOrDefault(x => x.IdStep == dbExecutionPointer.ExecutionPointer.IdStep);

		if (step == null)
			throw new InvalidOperationException($"{nameof(step)} == null");

		return dbExecutionPointer.ExecutionPointer.ToExecutionPointer(step);
	}

	public async Task UpdateExecutionPointerAsync(ExecutionPointer executionPointer, IExecutionPointerUpdate update, ITransactionController transactionController)
	{
		if (executionPointer == null)
			throw new ArgumentNullException(nameof(executionPointer));

		if (transactionController == null)
			throw new ArgumentNullException(nameof(transactionController));

		var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
		var martenSession = tc.CreateOrGetSession();
		var dbExecutionPointer = await martenSession.LoadAsync<DbExecutionPointer>(executionPointer.IdExecutionPointer).ConfigureAwait(false);

		if (dbExecutionPointer == null)
			throw new InvalidOperationException($"{nameof(dbExecutionPointer)} == null");

		dbExecutionPointer.ExecutionPointer = new ExecutionPointerDto(executionPointer).Update(update);
		martenSession.Store(dbExecutionPointer);
	}

	public async Task UpdateOrchestrationStatusAsync(Guid idOrchestrationInstance, OrchestrationStatus status, DateTime? completeTimeUtc, ITransactionController transactionController)
	{
		if (transactionController == null)
			throw new ArgumentNullException(nameof(transactionController));

		var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
		var martenSession = tc.CreateOrGetSession();
		var dbOrchestrationInstance = await martenSession.LoadAsync<DbOrchestrationInstance>(idOrchestrationInstance).ConfigureAwait(false);

		if (dbOrchestrationInstance == null)
			throw new InvalidOperationException($"{nameof(dbOrchestrationInstance)} == null");

		dbOrchestrationInstance.OrchestrationInstance.Status = status;
		dbOrchestrationInstance.OrchestrationInstance.CompleteTimeUtc = completeTimeUtc;
		martenSession.Store(dbOrchestrationInstance);
	}

	public async Task AddFinalizedBranchAsync(Guid idOrchestrationInstance, IOrchestrationStep finalizedBranch, ITransactionController transactionController, CancellationToken cancellationToken = default)
	{
		if (finalizedBranch == null)
			throw new ArgumentNullException(nameof(finalizedBranch));

		if (transactionController == null)
			throw new ArgumentNullException(nameof(transactionController));

		var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
		var martenSession = tc.CreateOrGetSession();
		var dbFinalizedBranches = await martenSession.LoadAsync<DbFinalizedBranches>(idOrchestrationInstance, cancellationToken).ConfigureAwait(false);

		if (dbFinalizedBranches == null)
			dbFinalizedBranches = new DbFinalizedBranches { IdOrchestrationInstance = idOrchestrationInstance };

		dbFinalizedBranches.StepIds.AddUniqueItem(finalizedBranch.IdStep);
		martenSession.Store(dbFinalizedBranches);
	}

	public async Task<List<Guid>> GetFinalizedBrancheIdsAsync(Guid idOrchestrationInstance, ITransactionController transactionController, CancellationToken cancellationToken = default)
	{
		if (transactionController == null)
			throw new ArgumentNullException(nameof(transactionController));

		var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
		var martenSession = tc.CreateOrGetSession();
		var dbFinalizedBranches = await martenSession.LoadAsync<DbFinalizedBranches>(idOrchestrationInstance, cancellationToken).ConfigureAwait(false);

		return dbFinalizedBranches?.StepIds ?? new List<Guid>();
	}










	public Task<IResult> SaveNewEventAsync(OrchestrationEvent @event, ITraceInfo traceInfo, ITransactionController transactionController, CancellationToken cancellationToken)
	{
		var result = new ResultBuilder();
		traceInfo = TraceInfo.Create(traceInfo);

		if (@event == null)
			return Task.FromResult((IResult)result.WithArgumentNullException(traceInfo, nameof(@event)));

		if (transactionController == null)
			return Task.FromResult((IResult)result.WithArgumentNullException(traceInfo, nameof(transactionController)));

		var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
		var martenSession = tc.CreateOrGetSession();
		martenSession.Store(
			new DbOrchestrationEvent
			{
				OrchestrationKey= @event.OrchestrationKey,
				MessageId = @event.Id,
				OrchestrationEventMessage = @event
			});

		return Task.FromResult((IResult)result.Build());
	}

	public async Task<IResult<List<OrchestrationEvent>?>> GetUnprocessedEventsAsync(string orchestrationKey, ITraceInfo traceInfo, ITransactionController transactionController, CancellationToken cancellationToken)
	{
		var result = new ResultBuilder<List<OrchestrationEvent>?>();
		traceInfo = TraceInfo.Create(traceInfo);

		if (transactionController == null)
			return result.WithArgumentNullException(traceInfo, nameof(transactionController));

		var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
		var martenSession = tc.CreateOrGetSession();
		var unprocessedEvents = await martenSession.QueryAsync(new UnprocessedEventsQuery(), cancellationToken).ConfigureAwait(false);

		return
			result
				.WithData(unprocessedEvents?.Select(x => x.OrchestrationEventMessage).ToList() ?? new List<OrchestrationEvent>())
				.Build();
	}

	public async Task<IResult> SetProcessedUtcAsync(OrchestrationEvent @event, ITraceInfo traceInfo, ITransactionController transactionController, CancellationToken cancellationToken)
	{
		var result = new ResultBuilder();
		traceInfo = TraceInfo.Create(traceInfo);

		if (@event == null)
			return result.WithArgumentNullException(traceInfo, nameof(@event));

		if (transactionController == null)
			return result.WithArgumentNullException(traceInfo, nameof(transactionController));

		var tc = transactionController.GetTransactionCache<PostgreSqlTransactionDocumentSessionCache>();
		var martenSession = tc.CreateOrGetSession();
		var existingEvent = await martenSession.LoadAsync<DbOrchestrationEvent>(@event.Id, cancellationToken).ConfigureAwait(false);

		if (existingEvent == null)
			return result.WithInvalidOperationException(traceInfo, $"{nameof(existingEvent)} == null");

		existingEvent.OrchestrationEventMessage.ProcessedUtc = @event.ProcessedUtc;
		martenSession.Store(existingEvent);

		return result.Build();
	}
}
