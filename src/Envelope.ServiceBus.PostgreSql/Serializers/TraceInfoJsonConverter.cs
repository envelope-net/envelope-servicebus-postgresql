using Envelope.Serializer.JsonConverters.Model;
using Envelope.Trace;

namespace Envelope.ServiceBus.PostgreSql.Serializers;

public class TraceInfoJsonConverter : Newtonsoft.Json.JsonConverter<ITraceInfo>
{
	public override void WriteJson(Newtonsoft.Json.JsonWriter writer, ITraceInfo? value, Newtonsoft.Json.JsonSerializer serializer)
	{
		throw new NotImplementedException("Read only converter");
	}

	public override ITraceInfo? ReadJson(Newtonsoft.Json.JsonReader reader, Type objectType, ITraceInfo? existingValue, bool hasExistingValue, Newtonsoft.Json.JsonSerializer serializer)
	{
		if (reader.TokenType == Newtonsoft.Json.JsonToken.Null)
		{
			return null;
		}
		else
		{
			var obj = Newtonsoft.Json.Linq.JObject.Load(reader);

			var runtimeUniqueKey = obj.Value<string>(nameof(ITraceInfo.RuntimeUniqueKey));
			var sourceSystemName = obj.Value<string>(nameof(ITraceInfo.SourceSystemName));

			var traceFrame = obj[nameof(ITraceInfo.TraceFrame)];
			
			var idUser = obj.Value<string>(nameof(ITraceInfo.IdUser));
			var externalCorrelationId = obj.Value<string>(nameof(ITraceInfo.ExternalCorrelationId));
			var correlationId = obj.Value<string>(nameof(ITraceInfo.CorrelationId));

			var contextProperties = obj[nameof(ITraceInfo.ContextProperties)];

			return new DeserializedTraceInfo
			{
				RuntimeUniqueKey = Guid.TryParse(runtimeUniqueKey, out var runtimeUniqueKeyGuid) ? runtimeUniqueKeyGuid : runtimeUniqueKeyGuid,
				SourceSystemName = sourceSystemName!,
				IdUser = idUser == null
					? null
					: (Guid.TryParse(idUser, out var idUserGuid) ? idUserGuid : idUserGuid),
				ExternalCorrelationId = externalCorrelationId,
				CorrelationId =
					Guid.TryParse(correlationId, out var correlationIdGuid) ? correlationIdGuid : correlationIdGuid,
				ContextProperties = contextProperties == null
					? new Dictionary<string, string?>()
					: (Dictionary<string, string?>)serializer.Deserialize(contextProperties!.CreateReader(), typeof(Dictionary<string, string?>))!,
				TraceFrame = (ITraceFrame)serializer.Deserialize(traceFrame!.CreateReader(), typeof(ITraceFrame))!,
				//Principal = ?,
				//User = ?
			};
		}
	}

	public override bool CanRead => true;

	public override bool CanWrite => false;
}
