//using Envelope.Database.PostgreSql;
//using Envelope.ServiceBus.Messages;
//using Envelope.ServiceBus.PostgreSql.Model;
//using NpgsqlTypes;

//namespace Envelope.ServiceBus.PostgreSql.Internal.DB;

//internal class DBExchangeMessage<TMessage> : DictionaryTable
//	where TMessage : class, IMessage
//{
//	public DBExchangeMessage()
//		: base(CreateDictionaryTableOptions())
//	{
//	}

//	public static IDictionaryTableOptions CreateDictionaryTableOptions()
//	{
//		var dictionaryTableOptions = new DictionaryTableOptions
//		{
//			SchemaName = "ESB",
//			TableName = "ExchangeMessage",
//			PropertyNames = new List<string>
//			{
//				nameof(ExchangeMessage<TMessage>.MessageId),
//				nameof(ExchangeMessage<TMessage>.Processed),
//				nameof(ExchangeMessage<TMessage>.ParentMessageId),
//				nameof(ExchangeMessage<TMessage>.PublishingTimeUtc),
//				nameof(ExchangeMessage<TMessage>.PublisherId),
//				nameof(ExchangeMessage<TMessage>.TraceInfo.RuntimeUniqueKey),
//				nameof(ExchangeMessage<TMessage>.TraceInfo.SourceSystemName),
//				nameof(ExchangeMessage<TMessage>.TraceInfo.TraceFrame),
//				nameof(ExchangeMessage<TMessage>.TraceInfo.IdUser),
//				nameof(ExchangeMessage<TMessage>.TraceInfo.CorrelationId),
//				nameof(ExchangeMessage<TMessage>.Timeout),
//				nameof(ExchangeMessage<TMessage>.TimeToLiveUtc),
//				nameof(ExchangeMessage<TMessage>.IdSession),
//				nameof(ExchangeMessage<TMessage>.ContentType),
//				nameof(ExchangeMessage<TMessage>.ContentEncoding),
//				nameof(ExchangeMessage<TMessage>.ContainsContent),
//				nameof(ExchangeMessage<TMessage>.IsCompressedContent),
//				nameof(ExchangeMessage<TMessage>.IsEncryptedContent),
//				nameof(ExchangeMessage<TMessage>.Priority),
//				nameof(ExchangeMessage<TMessage>.Headers),
//				nameof(ExchangeMessage<TMessage>.MessageStatus),
//				nameof(ExchangeMessage<TMessage>.RetryCount),
//				nameof(ExchangeMessage<TMessage>.ErrorHandling),
//				nameof(ExchangeMessage<TMessage>.DelayedToUtc),
//				nameof(ExchangeMessage<TMessage>.ExchangeName),
//				nameof(ExchangeMessage<TMessage>.IsAsynchronousInvocation),
//				nameof(ExchangeMessage<TMessage>.RoutingKey),
//				nameof(ExchangeMessage<TMessage>.TargetQueueName),
//				nameof(ExchangeMessage<TMessage>.DisableFaultQueue)
//			},

//			PropertyColumnMapping = new Dictionary<string, string>
//			{
//				{ nameof(ExchangeMessage<TMessage>.MessageId), "IdExchangeMessage" },
//				{ nameof(ExchangeMessage<TMessage>.ParentMessageId), "IdParentMessage" },
//				{ nameof(ExchangeMessage<TMessage>.PublisherId), "IdPublisher" },
//				{ nameof(ExchangeMessage<TMessage>.MessageStatus), "MessageStatusId" }
//			},

//			PropertyTypeMapping = new Dictionary<string, NpgsqlDbType>
//			{
//				{ nameof(ExchangeMessage<TMessage>.MessageId), NpgsqlDbType.Uuid },
//				{ nameof(ExchangeMessage<TMessage>.Processed), NpgsqlDbType.Boolean },
//				{ nameof(ExchangeMessage<TMessage>.ParentMessageId), NpgsqlDbType.Uuid },
//				{ nameof(ExchangeMessage<TMessage>.PublishingTimeUtc), NpgsqlDbType.TimestampTz },
//				{ nameof(ExchangeMessage<TMessage>.PublisherId), NpgsqlDbType.Varchar },
//				{ nameof(ExchangeMessage<TMessage>.TraceInfo.RuntimeUniqueKey), NpgsqlDbType.Uuid },
//				{ nameof(ExchangeMessage<TMessage>.TraceInfo.SourceSystemName), NpgsqlDbType.Varchar },
//				{ nameof(ExchangeMessage<TMessage>.TraceInfo.TraceFrame), NpgsqlDbType.Text },
//				{ nameof(ExchangeMessage<TMessage>.TraceInfo.IdUser), NpgsqlDbType.Uuid },
//				{ nameof(ExchangeMessage<TMessage>.TraceInfo.CorrelationId), NpgsqlDbType.Uuid },
//				{ nameof(ExchangeMessage<TMessage>.Timeout), NpgsqlDbType.Bigint },
//				{ nameof(ExchangeMessage<TMessage>.TimeToLiveUtc), NpgsqlDbType.TimestampTz },
//				{ nameof(ExchangeMessage<TMessage>.IdSession), NpgsqlDbType.Uuid },
//				{ nameof(ExchangeMessage<TMessage>.ContentType), NpgsqlDbType.Varchar },
//				{ nameof(ExchangeMessage<TMessage>.ContentEncoding), NpgsqlDbType.Varchar },
//				{ nameof(ExchangeMessage<TMessage>.ContainsContent), NpgsqlDbType.Boolean },
//				{ nameof(ExchangeMessage<TMessage>.IsCompressedContent), NpgsqlDbType.Boolean },
//				{ nameof(ExchangeMessage<TMessage>.IsEncryptedContent), NpgsqlDbType.Boolean },
//				{ nameof(ExchangeMessage<TMessage>.Priority), NpgsqlDbType.Integer },
//				{ nameof(ExchangeMessage<TMessage>.Headers), NpgsqlDbType.Jsonb },
//				{ nameof(ExchangeMessage<TMessage>.MessageStatus), NpgsqlDbType.Integer },
//				{ nameof(ExchangeMessage<TMessage>.RetryCount), NpgsqlDbType.Integer },
//				{ nameof(ExchangeMessage<TMessage>.ErrorHandling), NpgsqlDbType.Jsonb },
//				{ nameof(ExchangeMessage<TMessage>.DelayedToUtc), NpgsqlDbType.TimestampTz },
//				{ nameof(ExchangeMessage<TMessage>.ExchangeName), NpgsqlDbType.Varchar },
//				{ nameof(ExchangeMessage<TMessage>.IsAsynchronousInvocation), NpgsqlDbType.Boolean },
//				{ nameof(ExchangeMessage<TMessage>.RoutingKey), NpgsqlDbType.Varchar },
//				{ nameof(ExchangeMessage<TMessage>.TargetQueueName), NpgsqlDbType.Varchar },
//				{ nameof(ExchangeMessage<TMessage>.DisableFaultQueue), NpgsqlDbType.Boolean }
//			},

//			PropertyValueConverter = new Dictionary<string, Func<object?, object?>>
//			{
//				{
//					nameof(ExchangeMessage<TMessage>.MessageStatus),
//					obj =>
//					{
//						if (obj is not MessageStatus messageStatus)
//							return null;

//						return (int)messageStatus;
//					}
//				},
//				{
//					nameof(ExchangeMessage<TMessage>.Timeout),
//					obj =>
//					{
//						if (obj is not TimeSpan timeout)
//							return null;

//						return timeout.Ticks;
//					}
//				},
//				{
//					nameof(ExchangeMessage<TMessage>.Headers),
//					obj =>
//					{
//						if (obj is not IEnumerable<KeyValuePair<string, object>> headers)
//							return null;

//						return new Serializer.JsonSerializer().SerializeAsString(headers);
//					}
//				},
//				{
//					nameof(ExchangeMessage<TMessage>.ErrorHandling),
//					obj =>
//					{
//						if (obj is not IEnumerable<KeyValuePair<string, object>> errorHandling)
//							return null;

//						return new Serializer.JsonSerializer().SerializeAsString(errorHandling);
//					}
//				}
//			}
//		};

//		return dictionaryTableOptions;
//	}
//}
