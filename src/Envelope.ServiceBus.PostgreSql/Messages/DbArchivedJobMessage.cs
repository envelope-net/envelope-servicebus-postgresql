﻿using Envelope.Enums;
using Envelope.ServiceBus.Messages;
using Envelope.Trace;

namespace Envelope.ServiceBus.PostgreSql.Messages;

public class DbArchivedJobMessage : IJobMessage
{
	public Guid Id { get; set; }
	public int JobMessageTypeId { get; set; }
	public DateTime CreatedUtc { get; set; }
	public DateTime LastUpdatedUtc { get; set; }
	public ITraceInfo CreatedTraceInfo { get; set; }
	public ITraceInfo LastUpdatedTraceInfo { get; set; }
	public Guid TaskCorrelationId { get; set; }
	public int Priority { get; set; }
	public DateTime? TimeToLive { get; set; }
	public DateTime? DeletedUtc { get; set; }
	public int RetryCount { get; set; }
	public DateTime? DelayedToUtc { get; set; }
	public TimeSpan? LastDelay { get; set; }
	public int Status { get; set; }
	public DateTime? LastResumedUtc { get; set; }

	public string? EntityName { get; set; }
	public Guid? EntityId { get; set; }
	public Dictionary<string, object?>? Properties { get; set; }
	public string? Detail { get; set; }
	public bool IsDetailJson { get; set; }

	public IJobMessage Clone()
		=> JobMessageFactory.Clone(this);

	public object? GetProperty(string key, object? defaultValue = default)
	{
		if (Properties == null)
			return defaultValue;

		if (Properties.TryGetValue(key, out var value))
			return value;

		return defaultValue;
	}

	public T? GetProperty<T>(string key, T? defaultValue = default)
	{
		if (Properties == null)
			return defaultValue;

		if (Properties.TryGetValue(key, out var value))
			return (T)value;

		return defaultValue;
	}

	public void CopyFrom(IJobMessage from)
	{
		if (from == null)
			throw new ArgumentNullException(nameof(from));

		Id = from.Id;
		JobMessageTypeId = from.JobMessageTypeId;
		CreatedUtc = from.CreatedUtc;
		LastUpdatedUtc = from.LastUpdatedUtc;
		CreatedTraceInfo = from.CreatedTraceInfo;
		LastUpdatedTraceInfo = from.LastUpdatedTraceInfo;
		TaskCorrelationId = from.TaskCorrelationId;
		Priority = from.Priority;
		TimeToLive = from.TimeToLive;
		DeletedUtc = from.DeletedUtc;
		RetryCount = from.RetryCount;
		DelayedToUtc = from.DelayedToUtc;
		LastDelay = from.LastDelay;
		Status = from.Status;
		LastResumedUtc = from.LastResumedUtc;
		EntityName = from.EntityName;
		EntityId = from.EntityId;
		Properties = from.Properties;
		Detail = from.Detail;
		IsDetailJson = from.IsDetailJson;
	}

	public void Complete(
		ITraceInfo traceInfo,
		Dictionary<string, object?>? properties = null,
		string? detail = null,
		bool? isDetailJson = null)
	{
		var clon = JobMessageFactory.CreateCompletedMessage(this, traceInfo, properties, detail, isDetailJson);
		if (clon != null)
			CopyFrom(clon);
	}

	public void SetErrorRetry(
		ITraceInfo traceInfo,
		DateTime? delayedToUtc,
		TimeSpan? delay,
		int maxRetryCount,
		Dictionary<string, object?>? properties = null,
		string? detail = null,
		bool? isDetailJson = null)
	{
		var clon = JobMessageFactory.CreateErrorMessage(this, traceInfo, delayedToUtc, delay, maxRetryCount, properties, detail, isDetailJson);
		if (clon != null)
			CopyFrom(clon);
	}

	public void Suspend(
		ITraceInfo traceInfo,
		Dictionary<string, object?>? properties = null,
		string? detail = null,
		bool? isDetailJson = null)
	{
		var clon = JobMessageFactory.CreateSuspendedMessage(this, traceInfo, properties, detail, isDetailJson);
		if (clon != null)
			CopyFrom(clon);
	}

	public void Resume(
		ITraceInfo traceInfo,
		Dictionary<string, object?>? properties = null,
		string? detail = null,
		bool? isDetailJson = null)
	{
		var clon = JobMessageFactory.CreateResumedMessage(this, traceInfo, properties, detail, isDetailJson);
		if (clon != null)
			CopyFrom(clon);
	}

	public void Delete(
		ITraceInfo traceInfo,
		Dictionary<string, object?>? properties = null,
		string? detail = null,
		bool? isDetailJson = null)
	{
		var clon = JobMessageFactory.CreateDeletedMessage(this, traceInfo, properties, detail, isDetailJson);
		if (clon != null)
			CopyFrom(clon);
	}

	public string ToJson()
		=> Newtonsoft.Json.JsonConvert.SerializeObject(this, Newtonsoft.Json.Formatting.Indented);

	public override string ToString()
		=> $" {JobMessageTypeId} - {EnumHelper.ConvertIntToEnum<JobMessageStatus>(Status)}{(string.IsNullOrWhiteSpace(EntityName) ? "" : $" | {EntityName}[{EntityId}]")}";
}
