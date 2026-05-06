namespace MassTransit.NHibernateIntegration.Outbox
{
    using NHibernate;
    using NHibernate.Mapping.ByCode;
    using NHibernate.Mapping.ByCode.Conformist;


    public class OutboxMessageMap :
        ClassMapping<OutboxMessage>
    {
        public OutboxMessageMap()
        {
            Table("OutboxMessage");

            Id(x => x.SequenceNumber, m => m.Generator(Generators.Identity));

            Version(x => x.Version, m =>
            {
                m.UnsavedValue(0);
                m.Column("Version");
            });

            Property(x => x.EnqueueTime);
            Property(x => x.SentTime);

            Property(x => x.Headers, m => m.Type(NHibernateUtil.StringClob));
            Property(x => x.Properties, m => m.Type(NHibernateUtil.StringClob));

            Property(x => x.InboxMessageId, m => m.Column("InboxMessageId"));
            Property(x => x.InboxConsumerId, m => m.Column("InboxConsumerId"));

            Property(x => x.OutboxId, m => m.Column("OutboxId"));

            Property(x => x.MessageId);

            Property(x => x.ContentType, m => m.Length(256));
            Property(x => x.MessageType, m => m.Type(NHibernateUtil.StringClob));

            Property(x => x.Body, m => m.Type(NHibernateUtil.StringClob));

            Property(x => x.ConversationId);
            Property(x => x.CorrelationId);
            Property(x => x.InitiatorId);
            Property(x => x.RequestId);

            Property(x => x.SourceAddress, m => m.Type<UriUserType>());
            Property(x => x.DestinationAddress, m => m.Type<UriUserType>());
            Property(x => x.ResponseAddress, m => m.Type<UriUserType>());
            Property(x => x.FaultAddress, m => m.Type<UriUserType>());

            Property(x => x.ExpirationTime);
        }
    }
}

