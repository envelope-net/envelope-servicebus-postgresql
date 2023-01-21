using Envelope.Infrastructure;

namespace Envelope.ServiceBus.PostgreSql.Serializars;

public class EnvironmentInfoJsonConverter : Newtonsoft.Json.JsonConverter<EnvironmentInfo>
{
	public override void WriteJson(Newtonsoft.Json.JsonWriter writer, EnvironmentInfo value, Newtonsoft.Json.JsonSerializer serializer)
	{
		throw new NotImplementedException("Read only converter");
	}

	public override EnvironmentInfo ReadJson(Newtonsoft.Json.JsonReader reader, Type objectType, EnvironmentInfo existingValue, bool hasExistingValue, Newtonsoft.Json.JsonSerializer serializer)
	{
		if (reader.TokenType == Newtonsoft.Json.JsonToken.Null)
		{
			return default;
		}
		else
		{
			var obj = Newtonsoft.Json.Linq.JObject.Load(reader);

			var runningEnvironment = obj.Value<string>(nameof(EnvironmentInfo.RunningEnvironment));
			var createdUtc = obj.Value<string>(nameof(EnvironmentInfo.CreatedUtc));
			var frameworkDescription = obj.Value<string>(nameof(EnvironmentInfo.FrameworkDescription));
			var targetFramework = obj.Value<string>(nameof(EnvironmentInfo.TargetFramework));
			var clrVersion = obj.Value<string>(nameof(EnvironmentInfo.CLRVersion));
			var entryAssemblyName = obj.Value<string>(nameof(EnvironmentInfo.EntryAssemblyName));
			var entryAssemblyVersion = obj.Value<string>(nameof(EnvironmentInfo.EntryAssemblyVersion));
			var baseDirectory = obj.Value<string>(nameof(EnvironmentInfo.BaseDirectory));
			var machineName = obj.Value<string>(nameof(EnvironmentInfo.MachineName));
			var processName = obj.Value<string>(nameof(EnvironmentInfo.ProcessName));
			var processId = obj.Value<string>(nameof(EnvironmentInfo.ProcessId));
			var currentAppDomainName = obj.Value<string>(nameof(EnvironmentInfo.CurrentAppDomainName));
			var is64BitOperatingSystem = obj.Value<string>(nameof(EnvironmentInfo.Is64BitOperatingSystem));
			var is64BitProcess = obj.Value<string>(nameof(EnvironmentInfo.Is64BitProcess));
			var operatingSystemPlatform = obj.Value<string>(nameof(EnvironmentInfo.OperatingSystemPlatform));
			var operatingSystemVersion = obj.Value<string>(nameof(EnvironmentInfo.OperatingSystemVersion));
			var operatingSystemArchitecture = obj.Value<string>(nameof(EnvironmentInfo.OperatingSystemArchitecture));
			var processArchitecture = obj.Value<string>(nameof(EnvironmentInfo.ProcessArchitecture));
			var commandLine = obj.Value<string>(nameof(EnvironmentInfo.CommandLine));
			var applicationName = obj.Value<string>(nameof(EnvironmentInfo.ApplicationName));

			return new EnvironmentInfo(
			 	runningEnvironment,
				DateTimeOffset.TryParse(createdUtc, out var createdUtcDateTime) ? createdUtcDateTime : createdUtcDateTime,
				frameworkDescription,
				targetFramework,
				clrVersion,
				entryAssemblyName,
				entryAssemblyVersion,
				baseDirectory,
				machineName,
				processName,
				int.TryParse(processId, out var processIdInt) ? processIdInt : processIdInt,
				currentAppDomainName,
				bool.TryParse(is64BitOperatingSystem, out var is64BitOperatingSystemBool) ? is64BitOperatingSystemBool : is64BitOperatingSystemBool,
				bool.TryParse(is64BitProcess, out var is64BitProcessBool) ? is64BitProcessBool : is64BitProcessBool,
				operatingSystemPlatform,
				operatingSystemVersion,
				operatingSystemArchitecture,
				processArchitecture,
				commandLine,
				applicationName!);
		}
	}

	public override bool CanRead => true;

	public override bool CanWrite => false;
}
