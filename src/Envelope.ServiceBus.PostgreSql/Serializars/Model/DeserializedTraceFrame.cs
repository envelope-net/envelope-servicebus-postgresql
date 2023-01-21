using Envelope.Trace;
using System.Text;

namespace Envelope.ServiceBus.PostgreSql.Serializars.Model;

internal class DeserializedTraceFrame : ITraceFrame
{
	public Guid MethodCallId { get; set; }
	public string? CallerMemberName { get; set; }
	public string? CallerFilePath { get; set; }
	public int? CallerLineNumber { get; set; }
	public IEnumerable<MethodParameter>? MethodParameters { get; set; }
	public ITraceFrame? Previous { get; set; }

	IReadOnlyList<ITraceFrame> ITraceFrame.GetTrace()
		=> throw new NotImplementedException();

	IReadOnlyList<string> ITraceFrame.GetTraceMethodIdentifiers()
		=> throw new NotImplementedException();

	StringBuilder ITraceFrame.ToTraceString()
		=> throw new NotImplementedException();

	string ITraceFrame.ToTraceStringWithMethodParameters()
		=> throw new NotImplementedException();
}
