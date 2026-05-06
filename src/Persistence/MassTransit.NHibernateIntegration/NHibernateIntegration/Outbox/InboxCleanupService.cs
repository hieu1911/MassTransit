namespace MassTransit.NHibernateIntegration.Outbox
{
    using System;
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

        public InboxCleanupService(IOptions<InboxCleanupServiceOptions> options, ILogger<InboxCleanupService> logger, IServiceProvider provider)
        {
            _options = options.Value;
            _logger = logger;
            _provider = provider;

            _retryPolicy = Retry.Exponential(1000, TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(3));
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
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

                    removed = await _retryPolicy.Retry(() => CleanUpInboxState(stoppingToken), stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
                catch (Exception exception)
                {
                    _logger.LogError(exception, "CleanUpInboxState faulted");
                }
            }
        }

        async Task<int> CleanUpInboxState(CancellationToken cancellationToken)
        {
            var scope = _provider.CreateAsyncScope();

            try
            {
                var sessionFactory = scope.ServiceProvider.GetRequiredService<ISessionFactory>();

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
                    _logger.LogDebug("Outbox Removed {Count} expired inbox messages", count);

                return count;
            }
            finally
            {
                await scope.DisposeAsync().ConfigureAwait(false);
            }
        }
    }
}

