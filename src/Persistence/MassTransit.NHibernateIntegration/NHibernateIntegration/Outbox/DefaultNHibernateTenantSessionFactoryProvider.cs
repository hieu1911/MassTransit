namespace MassTransit.NHibernateIntegration.Outbox
{
    using NHibernate;


    public class DefaultNHibernateTenantSessionFactoryProvider :
        INHibernateTenantSessionFactoryProvider
    {
        readonly ISessionFactory _sessionFactory;

        public DefaultNHibernateTenantSessionFactoryProvider(ISessionFactory sessionFactory)
        {
            _sessionFactory = sessionFactory;
        }

        public string PartitionKey => "default";

        public ISessionFactory GetSessionFactory()
        {
            return _sessionFactory;
        }

        public ISessionFactory GetSessionFactory(string partitionKey)
        {
            // Single-tenant fallback: the same session factory is used for all partitions.
            return _sessionFactory;
        }
    }
}
