namespace MassTransit.NHibernateIntegration.Outbox
{
    using System.Collections.Generic;


    /// <summary>
    /// Provides the list of tenant partitions that the outbox delivery background service must poll/deliver.
    /// In typical multi-tenant deployments, a partition key maps to a tenant connection string.
    /// </summary>
    public interface INHibernateTenantDatabaseFactory
    {
        IReadOnlyCollection<string> GetAllPartitionKey();
    }
}

