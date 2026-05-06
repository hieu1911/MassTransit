#nullable enable
namespace MassTransit.Configuration
{
    using System;
    using System.Data;
    using DependencyInjection;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.DependencyInjection.Extensions;
    using Middleware;
    using Middleware.Outbox;
    using NHibernate;
    using NHibernateIntegration;
    using NHibernateIntegration.Outbox;


    public class NHibernateOutboxConfigurator :
        INHibernateOutboxConfigurator
    {
        readonly IBusRegistrationConfigurator _configurator;
        IsolationLevel _isolationLevel;
        bool _registerInboxCleanupService;

        public NHibernateOutboxConfigurator(IBusRegistrationConfigurator configurator)
        {
            _configurator = configurator;

            _isolationLevel = IsolationLevel.RepeatableRead;
            _registerInboxCleanupService = true;
        }

        public TimeSpan DuplicateDetectionWindow { get; set; } = TimeSpan.FromMinutes(30);

        public IsolationLevel IsolationLevel
        {
            set => _isolationLevel = value;
        }

        public TimeSpan QueryDelay { get; set; } = TimeSpan.FromSeconds(10);

        public int QueryMessageLimit { get; set; } = 100;

        public TimeSpan QueryTimeout { get; set; } = TimeSpan.FromSeconds(30);

        public void DisableInboxCleanupService()
        {
            _registerInboxCleanupService = false;
        }

        public virtual void UseBusOutbox(Action<INHibernateBusOutboxConfigurator>? configure = null)
        {
            var busOutboxConfigurator = new NHibernateBusOutboxConfigurator(_configurator, this);

            busOutboxConfigurator.Configure(configure);
        }

        public void Configure(Action<INHibernateOutboxConfigurator>? configure)
        {
            configure?.Invoke(this);

            _configurator.TryAddScoped<IOutboxContextFactory<ISessionFactory>, NHibernateOutboxContextFactory>();
            _configurator.AddOptions<NHibernateOutboxOptions>().Configure(options =>
            {
                options.IsolationLevel = _isolationLevel;
            });

            if (_registerInboxCleanupService)
            {
                _configurator.AddHostedService<InboxCleanupService>();
                _configurator.AddOptions<InboxCleanupServiceOptions>().Configure(options =>
                {
                    options.DuplicateDetectionWindow = DuplicateDetectionWindow;
                    options.QueryMessageLimit = QueryMessageLimit;
                    options.QueryDelay = QueryDelay;
                    options.QueryTimeout = QueryTimeout;
                });
            }
        }
    }
}

