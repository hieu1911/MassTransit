#nullable enable
namespace MassTransit.NHibernateIntegration.Outbox;

using System;
using Clients;
using DependencyInjection;
using Middleware.Outbox;


public class NHibernateConsumeContextScopedBusContext<TBus> :
    NHibernateScopedBusContext<TBus>
    where TBus : class, IBus
{
    readonly TBus _bus;
    readonly IClientFactory _clientFactory;
    readonly ConsumeContext _consumeContext;
    readonly IServiceProvider _provider;

    public NHibernateConsumeContextScopedBusContext(TBus bus, NHibernate.ISessionFactory sessionFactory, IBusOutboxNotification notification,
        IClientFactory clientFactory, IServiceProvider provider, ConsumeContext consumeContext)
        : base(bus, sessionFactory, notification, clientFactory, provider)
    {
        _bus = bus;
        _clientFactory = clientFactory;
        _provider = provider;
        _consumeContext = consumeContext;
    }

    protected override IPublishEndpointProvider GetPublishEndpointProvider()
    {
        return new ScopedConsumePublishEndpointProvider(_bus, _consumeContext, _provider);
    }

    protected override ISendEndpointProvider GetSendEndpointProvider()
    {
        return new ScopedConsumeSendEndpointProvider(_bus, _consumeContext, _provider);
    }

    protected override ScopedClientFactory GetClientFactory()
    {
        return new ScopedClientFactory(new ClientFactory(new ScopedClientFactoryContext(_clientFactory, _provider)), _consumeContext);
    }
}

