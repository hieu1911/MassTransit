namespace MassTransit.NHibernateIntegration.Outbox
{
    using NHibernate.Mapping.ByCode;
    using NHibernate.Mapping.ByCode.Conformist;


    public class OutboxStateMap :
        ClassMapping<OutboxState>
    {
        public OutboxStateMap()
        {
            Table("OutboxState");

            Id(x => x.OutboxId, m => m.Generator(Generators.Assigned));

            Version(x => x.Version, m =>
            {
                m.UnsavedValue(0);
                m.Column("Version");
            });

            Property(x => x.LockId);
            Property(x => x.Created);
            Property(x => x.Delivered);
            Property(x => x.LastSequenceNumber);
        }
    }
}

