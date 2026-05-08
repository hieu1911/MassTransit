namespace MassTransit.NHibernateIntegration.Outbox
{
    using NHibernate;


    /// <summary>
    /// Provides tenant-partition metadata and the matching NHibernate session factory.
    /// Register a scoped implementation to select the correct tenant database per request/message.
    /// </summary>
    public interface INHibernateTenantSessionFactoryProvider
    {
        string PartitionKey { get; }
        ISessionFactory GetSessionFactory();
        /// <summary>
        /// Returns the session factory for a specific tenant partition.
        /// For multi-tenant outbox delivery, the host service will use this overload
        /// so it can deliver from multiple DBs without request/company ambient context.
        /// </summary>
        ISessionFactory GetSessionFactory(string partitionKey);
    }
}
