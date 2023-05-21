using Envelope.Enums;
using Envelope.Logging;
using Envelope.Logging.Serializers.JsonConverters;
using Envelope.Logging.Serializers.JsonConverters.Model;
using Envelope.Trace;
using Microsoft.Extensions.Logging;

namespace Envelope.ServiceBus.PostgreSql.Serializers;

public class LogMessageJsonConverter : Newtonsoft.Json.JsonConverter<ILogMessage>
{
	public override void WriteJson(Newtonsoft.Json.JsonWriter writer, ILogMessage? value, Newtonsoft.Json.JsonSerializer serializer)
	{
		throw new NotImplementedException("Read only converter");
	}

	public override ILogMessage? ReadJson(Newtonsoft.Json.JsonReader reader, Type objectType, ILogMessage? existingValue, bool hasExistingValue, Newtonsoft.Json.JsonSerializer serializer)
	{
		if (reader.TokenType == Newtonsoft.Json.JsonToken.Null)
		{
			return null;
		}
		else
		{
			var obj = Newtonsoft.Json.Linq.JObject.Load(reader);

			var idLogMessage = obj.Value<string>(nameof(ILogMessage.IdLogMessage));
			var idLogLevel = obj.Value<string>(nameof(ILogMessage.IdLogLevel));
			var createdUtc = obj[nameof(ILogMessage.CreatedUtc)];
			
			var traceInfo = obj[nameof(ILogMessage.TraceInfo)];

			var logCode = obj.Value<string>(nameof(ILogMessage.LogCode));
			var clientMessage = obj.Value<string>(nameof(ILogMessage.ClientMessage));
			var internalMessage = obj.Value<string>(nameof(ILogMessage.InternalMessage));
			var stackTrace = obj.Value<string>(nameof(ILogMessage.StackTrace));
			var detail = obj.Value<string>(nameof(ILogMessage.Detail));
			var commandQueryName = obj.Value<string>(nameof(ILogMessage.CommandQueryName));
			var idCommandQuery = obj.Value<string>(nameof(ILogMessage.IdCommandQuery));
			//var isLogged = obj.Value<string>(nameof(ILogMessage.IsLogged));
			var methodCallElapsedMilliseconds = obj.Value<string>(nameof(ILogMessage.MethodCallElapsedMilliseconds));
			var propertyName = obj.Value<string>(nameof(ILogMessage.PropertyName));
			//var validationFailure = obj.Value<?>(nameof(ILogMessage.ValidationFailure));
			var displayPropertyName = obj.Value<string>(nameof(ILogMessage.DisplayPropertyName));
			var isValidationError = obj.Value<string>(nameof(ILogMessage.IsValidationError));

			var tags = obj[nameof(ILogMessage.Tags)];

			return new DeserializedLogMessage
			{
				IdLogMessage = Guid.TryParse(idLogMessage, out var idLogMessageGuid) ? idLogMessageGuid : idLogMessageGuid,
				LogLevel = EnumHelper.ConvertIntToEnum<LogLevel>(int.TryParse(idLogLevel, out var idLogLevelInt) ? idLogLevelInt : idLogLevelInt),
				//CreatedUtc = DateTimeOffset.TryParse(
				//	createdUtc,
				//	System.Globalization.CultureInfo.InvariantCulture.DateTimeFormat,
				//	System.Globalization.DateTimeStyles.None,
				//	out var createdUtcDateTime) ? createdUtcDateTime : createdUtcDateTime,
				CreatedUtc = createdUtc == null
					? default
					: (DateTimeOffset?)serializer.Deserialize(createdUtc.CreateReader(), typeof(DateTimeOffset)) ?? default,
				LogCode = logCode,
				ClientMessage = clientMessage,
				InternalMessage = internalMessage,
				StackTrace = stackTrace,
				Detail = detail,
				CommandQueryName = commandQueryName,
				IdCommandQuery = idCommandQuery == null
					? null
					: (Guid.TryParse(idCommandQuery, out var idCommandQueryGuid) ? idCommandQueryGuid : idCommandQueryGuid),
				MethodCallElapsedMilliseconds = decimal.TryParse(methodCallElapsedMilliseconds, out var methodCallElapsedMillisecondsDecimal) ? methodCallElapsedMillisecondsDecimal : methodCallElapsedMillisecondsDecimal,
				PropertyName = propertyName,
				//ValidationFailure = validationFailure,
				DisplayPropertyName = displayPropertyName,
				IsValidationError = bool.TryParse(isValidationError, out var isValidationErrorBool) ? isValidationErrorBool : isValidationErrorBool,
				Tags = tags == null ? null : (List<string>)serializer.Deserialize(tags!.CreateReader(), typeof(List<string>))!,
				TraceInfo = (ITraceInfo)serializer.Deserialize(traceInfo!.CreateReader(), typeof(ITraceInfo))!,
				//Exception = ?,
				//IsLogged = true,
				//ValidationFailure = ?
			};
		}
	}

	public override bool CanRead => true;

	public override bool CanWrite => false;
}
