#nullable enable
namespace MassTransit.NHibernateIntegration.Outbox
{
    using System;
    using System.Threading.Tasks;
    using Clients;
    using DependencyInjection;
    using Middleware;
    using Middleware.Outbox;
    using NHibernate;
    using Serialization;
    using Transports;


    public class NHibernateScopedBusContext<TBus> :
        ScopedBusContext,
        OutboxSendContext,
        IDisposable
        where TBus : class, IBus
    {
        readonly TBus _bus;
        readonly IClientFactory _clientFactory;
        readonly IBusOutboxNotification _notification;
        readonly IServiceProvider _provider;
        readonly ISessionFactory _sessionFactory;

        bool _ownsSession;
        bool _ownsTransaction;
        Guid _outboxId;
        bool _outboxStateCreated;
        IPublishEndpoint? _publishEndpoint;
        IScopedClientFactory? _scopedClientFactory;
        ISendEndpointProvider? _sendEndpointProvider;
        ISession? _session;
        ITransaction? _transaction;

        public NHibernateScopedBusContext(TBus bus, ISessionFactory sessionFactory, IBusOutboxNotification notification, IClientFactory clientFactory,
            IServiceProvider provider)
        {
            _bus = bus;
            _sessionFactory = sessionFactory;
            _notification = notification;
            _clientFactory = clientFactory;
            _provider = provider;
        }

        public void Dispose()
        {
            try
            {
                if (_ownsTransaction && _transaction is { IsActive: true })
                    _transaction.Commit();

                if (_outboxStateCreated && (_transaction?.WasCommitted ?? false))
                    _notification.Delivered();
            }
            finally
            {
                _transaction?.Dispose();
                if (_ownsSession)
                    _session?.Dispose();
            }
        }

        public async Task AddSend<T>(SendContext<T> context)
            where T : class
        {
            await EnsureOutboxState(context.CancellationToken).ConfigureAwait(false);

            if (_session == null)
                throw new InvalidOperationException("NHibernate session is not available for the outbox.");

            await _session.AddSend(context, SystemTextJsonMessageSerializer.Instance, outboxId: _outboxId).ConfigureAwait(false);
        }

        public object? GetService(Type serviceType)
        {
            return _provider.GetService(serviceType);
        }

        public ISendEndpointProvider SendEndpointProvider => _sendEndpointProvider ??= new OutboxSendEndpointProvider(this, GetSendEndpointProvider());

        public IPublishEndpoint PublishEndpoint =>
            _publishEndpoint ??= new PublishEndpoint(new OutboxPublishEndpointProvider(this, GetPublishEndpointProvider()));

        public IScopedClientFactory ClientFactory => _scopedClientFactory ??= GetClientFactory();

        async Task EnsureOutboxState(System.Threading.CancellationToken cancellationToken)
        {
            if (HasActiveOutbox())
                return;

            if (_transaction?.WasCommitted ?? false)
                _notification.Delivered();

            var scopedSession = _provider.GetService(typeof(ISession)) as ISession;
            _session = scopedSession ?? _sessionFactory.OpenSession();
            _ownsSession = scopedSession == null;

            _transaction = _session.GetCurrentTransaction();
            if (_transaction == null || _transaction.IsActive == false)
            {
                _transaction = _session.BeginTransaction();
                _ownsTransaction = true;
            }
            else
                _ownsTransaction = false;

            _outboxId = NewId.NextGuid();

            await _session.SaveAsync(new OutboxState
            {
                OutboxId = _outboxId,
                Created = DateTime.UtcNow
            }, cancellationToken).ConfigureAwait(false);

            _outboxStateCreated = true;
        }

        bool HasActiveOutbox()
        {
            return _outboxStateCreated
                && _session != null
                && _transaction != null
                && _transaction.IsActive
                && (_transaction.WasCommitted == false);
        }

        protected virtual ScopedClientFactory GetClientFactory()
        {
            return new ScopedClientFactory(new ClientFactory(new ScopedClientFactoryContext(_clientFactory, _provider)), null);
        }

        protected virtual IPublishEndpointProvider GetPublishEndpointProvider()
        {
            return _bus;
        }

        protected virtual ISendEndpointProvider GetSendEndpointProvider()
        {
            return _bus;
        }
    }
}

