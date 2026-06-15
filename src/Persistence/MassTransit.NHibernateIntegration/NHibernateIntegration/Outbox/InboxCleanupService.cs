namespace MassTransit.NHibernateIntegration.Outbox
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Hosting;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Options;
    using RetryPolicies;
    using NHibernate;


    public class InboxCleanupService :
        BackgroundService
    {
        readonly ILogger<InboxCleanupService> _logger;
        readonly InboxCleanupServiceOptions _options;
        readonly IServiceProvider _provider;
        readonly IRetryPolicy _retryPolicy;
        readonly INHibernateTenantDatabaseFactory _tenantDatabaseFactory;
        readonly bool _useMultitenantDatabases;

        public InboxCleanupService(IOptions<InboxCleanupServiceOptions> options, ILogger<InboxCleanupService> logger, IServiceProvider provider,
            INHibernateTenantDatabaseFactory tenantDatabaseFactory, IOptions<NHibernateOutboxOptions> nhibernateOutboxOptions)
        {
            _options = options.Value;
            _logger = logger;
            _provider = provider;
            _tenantDatabaseFactory = tenantDatabaseFactory;
            _useMultitenantDatabases = nhibernateOutboxOptions.Value.UseMultitenantDatabases;

            _retryPolicy = Retry.Exponential(1000, TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(3));
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            IReadOnlyCollection<string> partitionKeys;
            if (_useMultitenantDatabases)
            {
                partitionKeys = _tenantDatabaseFactory.GetAllPartitionKey();
                if (partitionKeys == null || partitionKeys.Count == 0)
                    partitionKeys = new[] { "default" };
            }
            else
                partitionKeys = new[] { "default" };

            var tasks = partitionKeys.Select(partitionKey => TenantWorker(partitionKey, stoppingToken))
                .ToArray();

            await Task.WhenAll(tasks).ConfigureAwait(false);
        }

        async Task TenantWorker(string partitionKey, CancellationToken stoppingToken)
        {
            var removed = 0;
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    if (removed == 0)
                        await Task.Delay(_options.QueryDelay, stoppingToken).ConfigureAwait(false);
                    else
                        removed = 0;

                    removed = await _retryPolicy.Retry(() => CleanUpInboxState(partitionKey, stoppingToken), stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception exception)
                {
                    _logger.LogError(exception, "CleanUpInboxState faulted for partition {PartitionKey}", partitionKey);
                }
            }
        }

        async Task<int> CleanUpInboxState(string partitionKey, CancellationToken cancellationToken)
        {
            var scope = _provider.CreateAsyncScope();

            try
            {
                var tenantSessionFactoryProvider = scope.ServiceProvider.GetRequiredService<INHibernateTenantSessionFactoryProvider>();
                var sessionFactory = _useMultitenantDatabases
                    ? tenantSessionFactoryProvider.GetSessionFactory(partitionKey)
                    : tenantSessionFactoryProvider.GetSessionFactory();

                using var session = sessionFactory.OpenSession();
                using var transaction = session.BeginTransaction();

                using var queryTimeout = new CancellationTokenSource(_options.QueryTimeout);
                using var queryToken = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, queryTimeout.Token);

                var removeTimestamp = DateTime.UtcNow - _options.DuplicateDetectionWindow;

                var count = await session.CreateQuery("delete from InboxState where Delivered is not null and Delivered < :removeTimestamp")
                    .SetParameter("removeTimestamp", removeTimestamp)
                    .SetMaxResults(_options.QueryMessageLimit)
                    .ExecuteUpdateAsync(queryToken.Token)
                    .ConfigureAwait(false);

                await transaction.CommitAsync(queryToken.Token).ConfigureAwait(false);

                if (count > 0)
                    _logger.LogDebug("Outbox Removed {Count} expired inbox messages from partition {PartitionKey}", count, partitionKey);

                return count;
            }
            finally
            {
                await scope.DisposeAsync().ConfigureAwait(false);
            }
        }
    }
}

