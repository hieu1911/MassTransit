#nullable enable
namespace MassTransit
{
    using System;
    using Configuration;
    using DependencyInjection;
    using NHibernate;
    using NHibernateIntegration;


    public static class NHibernateOutboxConfigurationExtensions
    {
        /// <summary>
        /// Configures the NHibernate outbox on the bus, which can subsequently be used to configure
        /// the transactional outbox on a receive endpoint.
        /// </summary>
        public static void AddNHibernateOutbox(this IBusRegistrationConfigurator configurator,
            Action<INHibernateOutboxConfigurator>? configure = null)
        {
            var outboxConfigurator = new NHibernateOutboxConfigurator(configurator);

            outboxConfigurator.Configure(configure);
        }

        /// <summary>
        /// Configure the NHibernate outbox on the receive endpoint
        /// </summary>
        public static void UseNHibernateOutbox(this IReceiveEndpointConfigurator configurator, IRegistrationContext context,
            Action<IOutboxOptionsConfigurator>? configure = null)
        {
            if (configurator == null)
                throw new ArgumentNullException(nameof(configurator));
            if (context == null)
                throw new ArgumentNullException(nameof(context));

            var observer = new OutboxConsumePipeSpecificationObserver<ISessionFactory>(configurator, context);

            configure?.Invoke(observer);

            configurator.ConnectConsumerConfigurationObserver(observer);
            configurator.ConnectSagaConfigurationObserver(observer);
        }

        [Obsolete("Use the IRegistrationContext overload instead. Visit https://masstransit.io/obsolete for details.")]
        public static void UseNHibernateOutbox(this IReceiveEndpointConfigurator configurator, IServiceProvider provider,
            Action<IOutboxOptionsConfigurator>? configure = null)
        {
            if (configurator == null)
                throw new ArgumentNullException(nameof(configurator));
            if (provider == null)
                throw new ArgumentNullException(nameof(provider));

            var observer = new OutboxConsumePipeSpecificationObserver<ISessionFactory>(configurator, provider, LegacySetScopedConsumeContext.Instance);

            configure?.Invoke(observer);

            configurator.ConnectConsumerConfigurationObserver(observer);
            configurator.ConnectSagaConfigurationObserver(observer);
        }
    }
}

