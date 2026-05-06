namespace MassTransit.NHibernateIntegration.Outbox
{
    using System;


    public static class NHibernateOutboxMappings
    {
        public static Type[] Mappings { get; } =
        {
            typeof(InboxStateMap),
            typeof(OutboxStateMap),
            typeof(OutboxMessageMap),
        };
    }
}

