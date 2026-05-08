namespace MassTransit.NHibernateIntegration.Outbox
{
    using System.Threading;
    using System.Threading.Tasks;


    public interface ITenantBusOutboxNotification
    {
        Task WaitForDelivery(string partitionKey, CancellationToken cancellationToken);
        void Delivered(string partitionKey);
    }
}
