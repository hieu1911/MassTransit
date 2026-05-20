#nullable enable
namespace MassTransit.NHibernateIntegration.Outbox
{
    using Clients;
    using DependencyInjection;
    using Microsoft.Extensions.Options;
    using Middleware;
    using Middleware.Outbox;
    using NHibernate;
    using Serialization;
    using System;
    using System.Threading.Tasks;
    using Transports;


    public class NHibernateScopedBusContext<TBus> :
        ScopedBusContext,
        OutboxSendContext,
        IDisposable
        where TBus : class, IBus
    {
        const string SingleDatabaseOutboxPartitionKey = "default";

        readonly TBus _bus;
        readonly IClientFactory _clientFactory;
        readonly ITenantBusOutboxNotification _notification;
        readonly IServiceProvider _provider;
        readonly INHibernateTenantSessionFactoryProvider _tenantSessionFactoryProvider;
        readonly bool _useMultitenantDatabases;

        bool _ownsSession;
        bool _ownsTransaction;
        Guid _outboxId;
        bool _outboxStateCreated;
        IPublishEndpoint? _publishEndpoint;
        IScopedClientFactory? _scopedClientFactory;
        ISendEndpointProvider? _sendEndpointProvider;
        ISession? _session;
        ITransaction? _transaction;

        public NHibernateScopedBusContext(TBus bus, INHibernateTenantSessionFactoryProvider tenantSessionFactoryProvider,
            ITenantBusOutboxNotification notification, IClientFactory clientFactory,
            IServiceProvider provider, IOptions<NHibernateOutboxOptions> outboxOptions)
        {
            _bus = bus;
            _tenantSessionFactoryProvider = tenantSessionFactoryProvider;
            _notification = notification;
            _clientFactory = clientFactory;
            _provider = provider;
            _useMultitenantDatabases = outboxOptions.Value.UseMultitenantDatabases;
        }

        public void Dispose()
        {
            try
            {
                if (_ownsTransaction && _transaction is { IsActive: true })
                    _transaction.Commit();

                if (_outboxStateCreated && ShouldNotifyDelivery())
                    _notification.Delivered(OutboxNotificationPartitionKey);
            }
            finally
            {
                if (_ownsTransaction)
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
                _notification.Delivered(OutboxNotificationPartitionKey);

            var sessionFactory = _useMultitenantDatabases
               ? _tenantSessionFactoryProvider.GetSessionFactory(_tenantSessionFactoryProvider.PartitionKey)
               : _tenantSessionFactoryProvider.GetSessionFactory();
            ISession? ambientSession = null;
            try
            {
                // Prefer NHibernate current session to avoid capturing an externally-owned ISession
                // from DI scope that may dispose it after publish/send scope exits.
                ambientSession = sessionFactory.GetCurrentSession();
            }
            catch (HibernateException)
            {
                // No current session is bound for this context.
            }

            if (ambientSession == null)
            {
                try
                {
                    // Try to get an ambient session from the provider, if available. This allows sharing an ambient session across scopes without relying on NHibernate's current session context.
                    var ambientProvider = _provider.GetService(typeof(INHibernateAmbientSessionProvider)) as INHibernateAmbientSessionProvider;
                    var borrowedSession = ambientProvider?.TryGetSession();
                    if (borrowedSession?.IsOpen == true)
                        ambientSession = borrowedSession;
                }
                catch
                {
                    // No ambient session provider or failed to get session from provider.
                }
            }

            if (ambientSession == null)
            {
                var scopedSession = _provider.GetService(typeof(ISession)) as ISession;
                if (scopedSession?.IsOpen == true)
                    ambientSession = scopedSession;
            }

            _session = ambientSession ?? sessionFactory.OpenSession();
            _ownsSession = ambientSession == null;

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

        string OutboxNotificationPartitionKey =>
            _useMultitenantDatabases ? _tenantSessionFactoryProvider.PartitionKey : SingleDatabaseOutboxPartitionKey;

        bool ShouldNotifyDelivery()
        {
            if (_outboxStateCreated == false)
                return false;

            // For ambient transactions, this scoped context can be disposed before the outer commit.
            // Wake the delivery worker so it can poll again after commit (spurious wake-up is harmless).
            if (_ownsTransaction == false)
                return true;

            return _transaction?.WasCommitted ?? false;
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

