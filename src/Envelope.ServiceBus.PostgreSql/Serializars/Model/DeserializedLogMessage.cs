using Envelope.Logging;
using Envelope.Serializer;
using Envelope.Trace;
using Microsoft.Extensions.Logging;
using System.Text;

namespace Envelope.ServiceBus.PostgreSql.Serializars.Model;

internal class DeserializedLogMessage : ILogMessage
{
	public Guid IdLogMessage { get; set; }

	public LogLevel LogLevel { get; set; }

	public int IdLogLevel => (int)LogLevel;

	public DateTimeOffset CreatedUtc { get; set; }

	public ITraceInfo TraceInfo { get; set; }

	public string? LogCode { get; set; }

	public string? ClientMessage { get; set; }

	public string? InternalMessage { get; set; }

	public string? StackTrace { get; set; }

	public string? Detail { get; set; }

	public string? CommandQueryName { get; set; }

	public Guid? IdCommandQuery { get; set; }

	public bool IsLogged { get; set; }

	public decimal? MethodCallElapsedMilliseconds { get; set; }

	public string? PropertyName { get; set; }

	public object? ValidationFailure { get; set; }

	public string? DisplayPropertyName { get; set; }

	public List<string>? Tags { get; set; }

	public bool IsValidationError { get; set; }




	public Exception? Exception { get; set; }

	public string ClientMessageWithId => ToString(true, false, false);

	public string ClientMessageWithIdAndPropName => ToString(true, true, false);

	public string FullMessage => ToString(true, true, true);

	bool ILogMessage.ShouldSerializeClientMessageWithId()
		=> throw new NotImplementedException();

	bool ILogMessage.ShouldSerializeClientMessageWithIdAndPropName()
		=> throw new NotImplementedException();

	bool ILogMessage.ShouldSerializeException()
		=> throw new NotImplementedException();

	bool ILogMessage.ShouldSerializeFullMessage()
		=> throw new NotImplementedException();

	bool ILogMessage.ShouldSerializeValidationFailure()
		=> throw new NotImplementedException();

	LogMessageDto ILogMessage.ToClientDto()
		=> throw new NotImplementedException();

	IDictionary<string, object?> IDictionaryObject.ToDictionary(ISerializer? serializer)
		=> throw new NotImplementedException();

	LogMessageDto ILogMessage.ToDto(params string[] ignoredPropterties)
		=> throw new NotImplementedException();

	Exception ILogMessage.ToException()
		=> throw new NotImplementedException();

	public string ToString(bool withId, bool withPropertyName, bool withDetail)
	{
		var sb = new StringBuilder();

		bool empty = string.IsNullOrWhiteSpace(ClientMessage);
		if (!empty)
			sb.Append(ClientMessage);

		if (withPropertyName)
		{
			if (!string.IsNullOrWhiteSpace(DisplayPropertyName))
			{
				if (empty)
					sb.Append(DisplayPropertyName);
				else
					sb.Append($" - {DisplayPropertyName}");

				empty = false;
			}
		}

		if (withId)
		{
			if (empty)
				sb.Append($"ID: {IdLogMessage}");
			else
				sb.Append($" (ID: {IdLogMessage})");

			empty = false;
		}

		if (withDetail && !string.IsNullOrWhiteSpace(InternalMessage))
		{
			if (empty)
				sb.Append(InternalMessage);
			else
				sb.Append($" | {InternalMessage}");
		}

		if (withDetail && !string.IsNullOrWhiteSpace(StackTrace))
		{
			if (empty)
				sb.Append(StackTrace);
			else
				sb.Append($" | {StackTrace}");

			empty = false;
		}

		if (withDetail && !string.IsNullOrWhiteSpace(Detail))
		{
			if (empty)
				sb.Append(Detail);
			else
				sb.Append($" | {Detail}");

			if (0 < TraceInfo.ContextProperties?.Count)
			{
				if (empty)
					sb.Append(string.Join("|", TraceInfo.ContextProperties.Select(x => $"{x.Key} = {x.Value}")));
				else
					sb.Append($" | {string.Join("|", TraceInfo.ContextProperties.Select(x => $"{x.Key} = {x.Value}"))}");
			}
		}

		return sb.ToString();
	}
}
