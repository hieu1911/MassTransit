using NHibernate;

namespace MassTransit.NHibernateIntegration.Outbox
{
    /// <summary>
    /// Try Get current ambient NHibernate session. This is used by the outbox repository to enlist in the same session/transaction as the consumer, if any.
    /// </summary>
    public interface INHibernateAmbientSessionProvider
    {
        ISession TryGetSession();
    }
}
