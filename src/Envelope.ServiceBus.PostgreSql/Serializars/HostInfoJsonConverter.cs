using Envelope.Enums;
using Envelope.Infrastructure;
using Envelope.ServiceBus.Hosts;
using Envelope.ServiceBus.PostgreSql.Serializars.Model;

namespace Envelope.ServiceBus.PostgreSql.Serializars;

public class HostInfoJsonConverter : Newtonsoft.Json.JsonConverter<IHostInfo>
{
	public override void WriteJson(Newtonsoft.Json.JsonWriter writer, IHostInfo? value, Newtonsoft.Json.JsonSerializer serializer)
	{
		throw new NotImplementedException("Read only converter");
	}

	public override IHostInfo? ReadJson(Newtonsoft.Json.JsonReader reader, Type objectType, IHostInfo? existingValue, bool hasExistingValue, Newtonsoft.Json.JsonSerializer serializer)
	{
		if (reader.TokenType == Newtonsoft.Json.JsonToken.Null)
		{
			return null;
		}
		else
		{
			var obj = Newtonsoft.Json.Linq.JObject.Load(reader);
			var hostId = obj.Value<string>(nameof(IHostInfo.HostId));
			var instanceId = obj.Value<string>(nameof(IHostInfo.InstanceId));
			var hostStatus = obj.Value<string>(nameof(IHostInfo.HostStatus));
			var environmentInfo = obj[nameof(IHostInfo.EnvironmentInfo)];

			return new DeserializedHostInfo
			{
				HostName = obj.Value<string>(nameof(IHostInfo.HostName))!,
				HostId = Guid.TryParse(hostId, out var hostIdGuid) ? hostIdGuid : hostIdGuid,
				InstanceId = Guid.TryParse(instanceId, out var instanceIdGuid) ? instanceIdGuid : instanceIdGuid,
				HostStatus = EnumHelper.ConvertIntToEnum<HostStatus>(int.TryParse(hostStatus, out var hostStatusInt) ? hostStatusInt : hostStatusInt),
				EnvironmentInfo = (EnvironmentInfo)serializer.Deserialize(environmentInfo!.CreateReader(), typeof(EnvironmentInfo))!
			};
		}
	}

	public override bool CanRead => true;

	public override bool CanWrite => false;
}
