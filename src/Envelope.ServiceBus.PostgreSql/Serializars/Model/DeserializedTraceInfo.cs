using Envelope.Identity;
using Envelope.Trace;

namespace Envelope.ServiceBus.PostgreSql.Serializars.Model;

internal class DeserializedTraceInfo : ITraceInfo
{
	public Guid RuntimeUniqueKey { get; set; }

	public string SourceSystemName { get; set; }

	public ITraceFrame TraceFrame { get; set; }

	public Guid? IdUser { get; set; }

	public string? ExternalCorrelationId { get; set; }

	public Guid? CorrelationId { get; set; }

	public Dictionary<string, string?> ContextProperties { get; set; }



	public EnvelopePrincipal? Principal { get; set; }

	public EnvelopeIdentity? User { get; set; }

	ITraceInfo ITraceInfo.RemoveContextProperty(string key)
		=> throw new NotImplementedException();

	ITraceInfo ITraceInfo.SetContextProperty(string key, string? value, bool force)
		=> throw new NotImplementedException();

	bool ITraceInfo.ShouldSerializePrincipal()
		=> throw new NotImplementedException();

	bool ITraceInfo.ShouldSerializeUser()
		=> throw new NotImplementedException();
}
