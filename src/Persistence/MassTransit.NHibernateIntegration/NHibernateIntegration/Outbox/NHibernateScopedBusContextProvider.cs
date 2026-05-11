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
            Bind<TBus, IClientFactory> clientFactory, ScopedConsumeContextProvider consumeContextProvider, IServiceProvider provider)
        {
            if (consumeContextProvider.HasContext)
                Context = new ConsumeContextScopedBusContext(consumeContextProvider.GetContext(), clientFactory.Value);
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

