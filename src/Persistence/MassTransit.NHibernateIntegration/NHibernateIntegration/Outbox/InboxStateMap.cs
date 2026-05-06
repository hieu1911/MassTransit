namespace MassTransit.NHibernateIntegration.Outbox
{
    using NHibernate.Mapping.ByCode;
    using NHibernate.Mapping.ByCode.Conformist;


    public class InboxStateMap :
        ClassMapping<InboxState>
    {
        public InboxStateMap()
        {
            Table("InboxState");

            Id(x => x.Id, m => m.Generator(Generators.Assigned));

            Version(x => x.Version, m =>
            {
                m.UnsavedValue(0);
                m.Column("Version");
            });

            Property(x => x.MessageId);
            Property(x => x.ConsumerId);
            Property(x => x.LockId);
            Property(x => x.Received);
            Property(x => x.ReceiveCount);
            Property(x => x.ExpirationTime);
            Property(x => x.Consumed);
            Property(x => x.Delivered);
            Property(x => x.LastSequenceNumber);

            NaturalId(n =>
            {
                n.Property(x => x.MessageId);
                n.Property(x => x.ConsumerId);
            });
        }
    }
}

