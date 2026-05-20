#nullable enable
namespace MassTransit.Configuration
{
    using DependencyInjection;
    using Microsoft.Extensions.DependencyInjection;
    using NHibernateIntegration.Outbox;
    using System;


    public class NHibernateBusOutboxConfigurator :
        INHibernateBusOutboxConfigurator
    {
        readonly IBusRegistrationConfigurator _configurator;
        readonly NHibernateOutboxConfigurator _outboxConfigurator;
        bool _registerOutboxDeliveryService;

        public NHibernateBusOutboxConfigurator(IBusRegistrationConfigurator configurator, NHibernateOutboxConfigurator outboxConfigurator)
        {
            _outboxConfigurator = outboxConfigurator;
            _configurator = configurator;

            _registerOutboxDeliveryService = true;
        }

        public int MessageDeliveryLimit { get; set; } = 100;

        public TimeSpan MessageDeliveryTimeout { get; set; } = TimeSpan.FromSeconds(10);

        public void DisableDeliveryService()
        {
            _registerOutboxDeliveryService = false;
        }

        public virtual void Configure(Action<INHibernateBusOutboxConfigurator>? configure)
        {
            configure?.Invoke(this);

            _configurator.ReplaceScoped<IScopedBusContextProvider<IBus>, NHibernateScopedBusContextProvider<IBus>>();
            _configurator.AddSingleton<ITenantBusOutboxNotification, TenantBusOutboxNotification>();

            if (_registerOutboxDeliveryService)
            {
                _configurator.AddHostedService<BusOutboxDeliveryService>();
                _configurator.AddOptions<OutboxDeliveryServiceOptions>()
                    .Configure(options =>
                    {
                        options.QueryDelay = _outboxConfigurator.QueryDelay;
                        options.QueryMessageLimit = _outboxConfigurator.QueryMessageLimit;
                        options.QueryTimeout = _outboxConfigurator.QueryTimeout;
                        options.MessageDeliveryLimit = MessageDeliveryLimit;
                        options.MessageDeliveryTimeout = MessageDeliveryTimeout;
                    });
            }
        }
    }
}

