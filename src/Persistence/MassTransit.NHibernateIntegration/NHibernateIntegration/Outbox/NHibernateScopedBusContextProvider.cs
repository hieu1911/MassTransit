#nullable enable
namespace MassTransit.NHibernateIntegration.Outbox
{
    using System;
    using DependencyInjection;
    using Middleware.Outbox;
    using NHibernate;


    public class NHibernateScopedBusContextProvider<TBus> :
        IScopedBusContextProvider<TBus>,
        IDisposable
        where TBus : class, IBus
    {
        public NHibernateScopedBusContextProvider(TBus bus, ISessionFactory sessionFactory, IBusOutboxNotification notification,
            Bind<TBus, IClientFactory> clientFactory, Bind<TBus, IScopedConsumeContextProvider> consumeContextProvider,
            IScopedConsumeContextProvider globalConsumeContextProvider, IServiceProvider provider)
        {
            if (consumeContextProvider.Value.HasContext)
                Context = new ConsumeContextScopedBusContext(consumeContextProvider.Value.GetContext(), clientFactory.Value);
            else if (globalConsumeContextProvider.HasContext)
            {
                Context = new NHibernateConsumeContextScopedBusContext<TBus>(bus, sessionFactory, notification, clientFactory.Value, provider,
                    globalConsumeContextProvider.GetContext());
            }
            else
                Context = new NHibernateScopedBusContext<TBus>(bus, sessionFactory, notification, clientFactory.Value, provider);
        }

        public void Dispose()
        {
            if (Context is IDisposable disposable)
                disposable.Dispose();
        }

        public ScopedBusContext Context { get; }
    }
}

