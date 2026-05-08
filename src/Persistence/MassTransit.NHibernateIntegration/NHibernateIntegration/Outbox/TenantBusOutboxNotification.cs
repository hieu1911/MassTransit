namespace MassTransit.NHibernateIntegration.Outbox
{
    using System;
    using System.Collections.Concurrent;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Options;
    using Middleware;


    public class TenantBusOutboxNotification :
        ITenantBusOutboxNotification
    {
        readonly object _lock = new object();
        readonly IOptions<OutboxDeliveryServiceOptions> _options;
        readonly ConcurrentDictionary<string, CancellationTokenSource> _cancellationTokenSources;

        public TenantBusOutboxNotification(IOptions<OutboxDeliveryServiceOptions> options)
        {
            _options = options;
            _cancellationTokenSources = new ConcurrentDictionary<string, CancellationTokenSource>(StringComparer.Ordinal);
        }

        public async Task WaitForDelivery(string partitionKey, CancellationToken cancellationToken)
        {
            var key = NormalizePartitionKey(partitionKey);
            CancellationTokenSource cancellationTokenSource;

            lock (_lock)
            {
                cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                _cancellationTokenSources[key] = cancellationTokenSource;
            }

            try
            {
                var delay = await Task.Delay(_options.Value.QueryDelay, cancellationTokenSource.Token)
                    .ContinueWith(t => t, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default)
                    .ConfigureAwait(false);

                if (delay.IsCanceled)
                    cancellationToken.ThrowIfCancellationRequested();
            }
            finally
            {
                lock (_lock)
                {
                    if (_cancellationTokenSources.TryGetValue(key, out var existing)
                        && ReferenceEquals(existing, cancellationTokenSource))
                    {
                        _cancellationTokenSources.TryRemove(key, out _);
                    }
                }

                cancellationTokenSource.Dispose();
            }
        }

        public void Delivered(string partitionKey)
        {
            var key = NormalizePartitionKey(partitionKey);
            if (_cancellationTokenSources.TryGetValue(key, out var cancellationTokenSource))
                cancellationTokenSource.Cancel();
        }

        static string NormalizePartitionKey(string partitionKey)
        {
            return string.IsNullOrWhiteSpace(partitionKey) ? "default" : partitionKey.Trim();
        }
    }
}
