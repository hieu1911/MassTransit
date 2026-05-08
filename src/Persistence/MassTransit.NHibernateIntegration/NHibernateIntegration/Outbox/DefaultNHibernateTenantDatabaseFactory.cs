namespace MassTransit.NHibernateIntegration.Outbox
{
    using System.Collections.Generic;


    public class DefaultNHibernateTenantDatabaseFactory :
        INHibernateTenantDatabaseFactory
    {
        public IReadOnlyCollection<string> GetAllConnectionStrings()
        {
            return new[] { "default" };
        }
    }
}

