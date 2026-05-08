#nullable enable
namespace MassTransit.NHibernateIntegration.Outbox
{
    using System;
    using DependencyInjection;


    public class NHibernateScopedBusContextProvider<TBus> :
        IScopedBusContextProvider<TBus>,
        IDisposable
        where TBus : class, IBus
    {
        public NHibernateScopedBusContextProvider(TBus bus, INHibernateTenantSessionFactoryProvider tenantSessionFactoryProvider,
            ITenantBusOutboxNotification notification,
            Bind<TBus, IClientFactory> clientFactory, Bind<TBus, IScopedConsumeContextProvider> consumeContextProvider,
            IScopedConsumeContextProvider globalConsumeContextProvider, IServiceProvider provider)
        {
            if (consumeContextProvider.Value.HasContext)
                Context = new ConsumeContextScopedBusContext(consumeContextProvider.Value.GetContext(), clientFactory.Value);
            else if (globalConsumeContextProvider.HasContext)
            {
                Context = new NHibernateConsumeContextScopedBusContext<TBus>(bus, tenantSessionFactoryProvider, notification, clientFactory.Value, provider,
                    globalConsumeContextProvider.GetContext());
            }
            else
                Context = new NHibernateScopedBusContext<TBus>(bus, tenantSessionFactoryProvider, notification, clientFactory.Value, provider);
        }

        public void Dispose()
        {
            if (Context is IDisposable disposable)
                disposable.Dispose();
        }

        public ScopedBusContext Context { get; }
    }
}

