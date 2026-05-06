namespace MassTransit.NHibernateIntegration.Outbox
{
    using System.Data;


    public class NHibernateOutboxOptions
    {
        public IsolationLevel IsolationLevel { get; set; } = IsolationLevel.RepeatableRead;
    }
}

