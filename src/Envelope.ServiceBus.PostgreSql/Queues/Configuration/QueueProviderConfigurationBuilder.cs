using Envelope.ServiceBus.Configuration;
using Envelope.ServiceBus.Messages;
using Envelope.ServiceBus.PostgreSql.Messages.Internal;
using Envelope.ServiceBus.PostgreSql.Queues.Internal;
using Envelope.ServiceBus.Queues;
using Envelope.ServiceBus.Queues.Configuration;

namespace Envelope.ServiceBus.PostgreSql.Queues.Configuration;

public interface INewConfigurationBuilder<TBuilder, TObject> : IQueueProviderConfigurationBuilder<TBuilder, TObject>
	where TBuilder : INewConfigurationBuilder<TBuilder, TObject>
	where TObject : IQueueProviderConfiguration
{
	TBuilder RegisterPostgreSqlQueue<TMessage>(HandleMessage<TMessage>? messageHandler, bool force = true)
		where TMessage : class, IMessage;
}

public abstract class NewConfigurationBuilderBase<TBuilder, TObject> : QueueProviderConfigurationBuilderBase<TBuilder, TObject>, INewConfigurationBuilder<TBuilder, TObject>
	where TBuilder : NewConfigurationBuilderBase<TBuilder, TObject>
	where TObject : IQueueProviderConfiguration
{
	protected NewConfigurationBuilderBase(TObject queueProviderConfiguration)
		: base(queueProviderConfiguration)
	{
	}

	public TBuilder RegisterPostgreSqlQueue<TMessage>(HandleMessage<TMessage>? messageHandler, bool force = true)
		where TMessage : class, IMessage
		=> RegisterQueue(
			typeof(TMessage).FullName!,
			sp =>
			{
				var messageQueueConfiguration = MessageQueueConfigurationBuilder<TMessage>
					.GetDefaultBuilder(_queueProviderConfiguration.ServiceBusOptions, messageHandler)
					.FIFOQueue((sp, maxSize) => new DbMessageQueue<TMessage>(true), true)
					.DelayableQueue((sp, maxSize) => new DbMessageQueue<TMessage>(false), true)
					.MessageBodyProvider(sp => new PostgreSqlMessageBodyProvider(), true)
					.Build();

				var context = new MessageQueueContext<TMessage>(messageQueueConfiguration, sp);
				return new MessageQueue<TMessage>(context);
			},
			force);
}

public class NewConfigurationBuilder : NewConfigurationBuilderBase<NewConfigurationBuilder, IQueueProviderConfiguration>
{
	public NewConfigurationBuilder(IServiceBusOptions serviceBusOptions)
		: base(new QueueProviderConfiguration(serviceBusOptions))
	{
	}

	private NewConfigurationBuilder(QueueProviderConfiguration queueProviderConfiguration)
		: base(queueProviderConfiguration)
	{
	}

	public static implicit operator QueueProviderConfiguration?(NewConfigurationBuilder builder)
	{
		if (builder == null)
			return null;

		return builder._queueProviderConfiguration as QueueProviderConfiguration;
	}

	public static implicit operator NewConfigurationBuilder?(QueueProviderConfiguration queueProviderConfiguration)
	{
		if (queueProviderConfiguration == null)
			return null;

		return new NewConfigurationBuilder(queueProviderConfiguration);
	}
}
