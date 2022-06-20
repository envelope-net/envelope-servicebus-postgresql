using Envelope.ServiceBus.Messages;
using Envelope.ServiceBus.PostgreSql.Messages.Internal;
using Envelope.ServiceBus.PostgreSql.Queues.Internal;
using Envelope.ServiceBus.Queues;
using Envelope.ServiceBus.Queues.Configuration;

namespace Envelope.ServiceBus.PostgreSql.Extensions;

public static class QueueProviderConfigurationBuilderExtensions
{
	public static QueueProviderConfigurationBuilder RegisterPostgreSqlQueue<TMessage>(
		this QueueProviderConfigurationBuilder builder,
		HandleMessage<TMessage>? messageHandler,
		bool force = true)
		where TMessage : class, IMessage
		=> builder.RegisterQueue(
			typeof(TMessage).FullName!,
			sp =>
			{
				var messageQueueConfiguration = MessageQueueConfigurationBuilder<TMessage>
					.GetDefaultBuilder(builder.Internal().ServiceBusOptions, messageHandler)
					.FIFOQueue((sp, maxSize) => new DbMessageQueue<TMessage>(true))
					.DelayableQueue((sp, maxSize) => new DbMessageQueue<TMessage>(false))
					.MessageBodyProvider(sp => new PostgreSqlMessageBodyProvider())
					.Build();

				var context = new MessageQueueContext<TMessage>(messageQueueConfiguration, sp);
				return new MessageQueue<TMessage>(context);
			},
			force);
}
