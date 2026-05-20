namespace MassTransit.NHibernateIntegration.Outbox
{
    using System.Data;


    public class NHibernateOutboxOptions
    {
        public IsolationLevel IsolationLevel { get; set; } = IsolationLevel.RepeatableRead;

        /// <summary>
        /// When true, inbox/outbox and delivery use tenant partition keys and <see cref="INHibernateTenantDatabaseFactory" /> so each tenant can use its own database.
        /// When false, a single database is used: session factories are resolved via <see cref="INHibernateTenantSessionFactoryProvider.GetSessionFactory" /> without per-partition routing, similar to the Entity Framework Core transactional outbox.
        /// </summary>
        public bool UseMultitenantDatabases { get; set; } = false;
    }
}

