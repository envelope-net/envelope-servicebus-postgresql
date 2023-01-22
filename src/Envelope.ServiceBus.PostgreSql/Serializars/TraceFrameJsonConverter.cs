using Envelope.ServiceBus.PostgreSql.Serializars.Model;
using Envelope.Trace;

namespace Envelope.ServiceBus.PostgreSql.Serializars;

public class TraceFrameJsonConverter : Newtonsoft.Json.JsonConverter<ITraceFrame>
{
	public override void WriteJson(Newtonsoft.Json.JsonWriter writer, ITraceFrame? value, Newtonsoft.Json.JsonSerializer serializer)
	{
		throw new NotImplementedException("Read only converter");
	}

	public override ITraceFrame? ReadJson(Newtonsoft.Json.JsonReader reader, Type objectType, ITraceFrame? existingValue, bool hasExistingValue, Newtonsoft.Json.JsonSerializer serializer)
	{
		if (reader.TokenType == Newtonsoft.Json.JsonToken.Null)
		{
			return null;
		}
		else
		{
			var obj = Newtonsoft.Json.Linq.JObject.Load(reader);

			var methodCallId = obj.Value<string>(nameof(ITraceFrame.MethodCallId));
			var callerMemberName = obj.Value<string>(nameof(ITraceFrame.CallerMemberName));
			var callerFilePath = obj.Value<string>(nameof(ITraceFrame.CallerFilePath));
			var callerLineNumber = obj.Value<string>(nameof(ITraceFrame.CallerLineNumber));

			//var methodParameters = obj.Value<?>(nameof(ITraceFrame.MethodParameters));

			var previous = obj[nameof(ITraceFrame.Previous)];

			return new DeserializedTraceFrame
			{
				MethodCallId = Guid.TryParse(methodCallId, out var methodCallIdGuid) ? methodCallIdGuid : methodCallIdGuid,
				CallerMemberName = callerMemberName,
				CallerFilePath = callerFilePath,
				CallerLineNumber = callerLineNumber == null
					? null
					: (int.TryParse(callerLineNumber, out var callerLineNumberInt) ? callerLineNumberInt : callerLineNumberInt),
				//MethodParameters = ?,
				Previous = previous != null
					? (ITraceFrame)serializer.Deserialize(previous!.CreateReader(), typeof(ITraceFrame))!
					: null
			};
		}
	}

	public override bool CanRead => true;

	public override bool CanWrite => false;
}
