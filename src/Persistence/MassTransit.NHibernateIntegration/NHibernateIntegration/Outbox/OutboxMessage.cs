#nullable enable
namespace MassTransit.NHibernateIntegration.Outbox
{
    using System;
    using System.Collections.Generic;
    using Metadata;
    using Middleware;
    using Serialization;


    public class OutboxMessage :
        OutboxMessageContext
    {
        Headers? _headers;
        IReadOnlyDictionary<string, object>? _properties;

        public virtual int Version { get; set; }

        public virtual DateTime? EnqueueTime { get; set; }

        public virtual DateTime SentTime { get; set; }

        public virtual string? Headers { get; set; }

        public virtual string? Properties { get; set; }

        public virtual Guid? InboxMessageId { get; set; }

        public virtual Guid? InboxConsumerId { get; set; }

        public virtual Guid? OutboxId { get; set; }

        public virtual long SequenceNumber { get; set; }

        public virtual Guid MessageId { get; set; }

        public virtual string ContentType { get; set; } = null!;
        public virtual string MessageType { get; set; } = null!;

        public virtual string Body { get; set; } = null!;

        public virtual Guid? ConversationId { get; set; }
        public virtual Guid? CorrelationId { get; set; }
        public virtual Guid? InitiatorId { get; set; }

        public virtual Guid? RequestId { get; set; }

        public virtual Uri? SourceAddress { get; set; }
        public virtual Uri? DestinationAddress { get; set; }
        public virtual Uri? ResponseAddress { get; set; }
        public virtual Uri? FaultAddress { get; set; }

        public virtual DateTime? ExpirationTime { get; set; }

        Guid? MessageContext.MessageId => MessageId;
        DateTime? MessageContext.SentTime => SentTime;
        Headers MessageContext.Headers => _headers ?? EmptyHeaders.Instance;
        HostInfo MessageContext.Host => HostMetadataCache.Host;

        IReadOnlyDictionary<string, object> OutboxMessageContext.Properties => _properties!;

        public virtual void Deserialize(IObjectDeserializer deserializer)
        {
            _headers = DeserializeHeaders(deserializer);
            _properties = DeserializeProperties(deserializer);
        }

        Headers DeserializeHeaders(IObjectDeserializer deserializer)
        {
            Dictionary<string, object?>? headers = deserializer.DeserializeDictionary<object?>(Headers);
            if (headers != null)
                return new DictionarySendHeaders(headers);

            return EmptyHeaders.Instance;
        }

        IReadOnlyDictionary<string, object> DeserializeProperties(IObjectDeserializer deserializer)
        {
            Dictionary<string, object>? properties = deserializer.DeserializeDictionary<object>(Properties);

            return properties ?? OutboxMessageStaticData.Empty;
        }
    }


    static class OutboxMessageStaticData
    {
        public static IReadOnlyDictionary<string, object> Empty { get; } = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
    }
}

