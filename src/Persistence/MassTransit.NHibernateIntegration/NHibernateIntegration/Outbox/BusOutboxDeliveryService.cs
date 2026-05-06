namespace MassTransit.NHibernateIntegration.Outbox
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Logging;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Hosting;
    using Microsoft.Extensions.Logging;
    using Middleware;
    using Microsoft.Extensions.Options;
    using Middleware.Outbox;
    using NHibernate;
    using RetryPolicies;
    using Serialization;


    public class BusOutboxDeliveryService :
        BackgroundService
    {
        readonly IBusControl _busControl;
        readonly ILogger _logger;
        readonly IBusOutboxNotification _notification;
        readonly OutboxDeliveryServiceOptions _options;
        readonly IServiceProvider _provider;
        readonly IRetryPolicy _retryPolicy;

        public BusOutboxDeliveryService(IBusControl busControl, IOptions<OutboxDeliveryServiceOptions> options,
            IBusOutboxNotification notification, ILogger<BusOutboxDeliveryService> logger, IServiceProvider provider)
        {
            _busControl = busControl;
            _notification = notification;
            _provider = provider;
            _logger = logger;
            _options = options.Value;

            _retryPolicy = Retry.Exponential(1000, TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(3));
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await _busControl.WaitForHealthStatus(BusHealthStatus.Healthy, stoppingToken).ConfigureAwait(false);

                    var count = await _retryPolicy.Retry(() => DeliverBatch(_options.QueryMessageLimit, stoppingToken), stoppingToken).ConfigureAwait(false);
                    if (count > 0)
                        continue;

                    await _notification.WaitForDelivery(stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
                catch (Exception exception)
                {
                    _logger.LogError(exception, "ProcessOutboxes faulted");
                }
            }
        }

        async Task<int> DeliverBatch(int resultLimit, CancellationToken cancellationToken)
        {
            var scope = _provider.CreateAsyncScope();
            try
            {
                var sessionFactory = scope.ServiceProvider.GetRequiredService<ISessionFactory>();
                var totalDelivered = 0;

                while (totalDelivered < resultLimit && !cancellationToken.IsCancellationRequested)
                {
                    using var session = sessionFactory.OpenSession();
                    using var transaction = session.BeginTransaction();

                    using var timeoutToken = new CancellationTokenSource(_options.QueryTimeout);
                    using var queryToken = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutToken.Token);

                    try
                    {
                        var outboxState = await session.CreateQuery("from OutboxState order by Created")
                            .SetLockMode("this", LockMode.Upgrade)
                            .SetMaxResults(1)
                            .UniqueResultAsync<OutboxState>(queryToken.Token)
                            .ConfigureAwait(false);

                        if (outboxState == null)
                        {
                            await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                            break;
                        }

                        outboxState.LockId = NewId.NextGuid();
                        await session.UpdateAsync(outboxState, queryToken.Token).ConfigureAwait(false);

                        int deliveredCount;
                        if (outboxState.Delivered.HasValue)
                        {
                            await RemoveOutbox(session, outboxState, queryToken.Token).ConfigureAwait(false);
                            deliveredCount = 0;
                        }
                        else
                            deliveredCount = await DeliverOutboxMessages(session, outboxState, queryToken.Token).ConfigureAwait(false);

                        await session.FlushAsync(queryToken.Token).ConfigureAwait(false);
                        await transaction.CommitAsync(queryToken.Token).ConfigureAwait(false);

                        if (deliveredCount <= 0)
                            continue;

                        totalDelivered += deliveredCount;
                    }
                    catch (Exception)
                    {
                        if (transaction.IsActive)
                            await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);

                        throw;
                    }
                }

                return totalDelivered;
            }
            finally
            {
                await scope.DisposeAsync().ConfigureAwait(false);
            }
        }

        static async Task RemoveOutbox(ISession session, OutboxState outboxState, CancellationToken cancellationToken)
        {
            IList<OutboxMessage> messages = await session.QueryOver<OutboxMessage>()
                .Where(x => x.OutboxId == outboxState.OutboxId)
                .ListAsync(cancellationToken)
                .ConfigureAwait(false);

            foreach (var message in messages)
                await session.DeleteAsync(message, cancellationToken).ConfigureAwait(false);

            await session.DeleteAsync(outboxState, cancellationToken).ConfigureAwait(false);

            if (messages.Count > 0)
                LogContext.Debug?.Log("Outbox removed {Count} messages: {OutboxId}", messages.Count, outboxState.OutboxId);
        }

        async Task<int> DeliverOutboxMessages(ISession session, OutboxState outboxState, CancellationToken cancellationToken)
        {
            var messageLimit = _options.MessageDeliveryLimit;

            var hasLastSequenceNumber = outboxState.LastSequenceNumber.HasValue;
            var lastSequenceNumber = outboxState.LastSequenceNumber ?? 0;

            IList<OutboxMessage> messages = await session.QueryOver<OutboxMessage>()
                .Where(x => x.OutboxId == outboxState.OutboxId && x.SequenceNumber > lastSequenceNumber)
                .OrderBy(x => x.SequenceNumber).Asc
                .Take(messageLimit + 1)
                .ListAsync(cancellationToken)
                .ConfigureAwait(false);

            var sentSequenceNumber = 0L;
            var deliveredCount = 0;
            var messageIndex = 0;

            for (; messageIndex < messages.Count && deliveredCount < messageLimit; messageIndex++)
            {
                var message = messages[messageIndex];
                message.Deserialize(SystemTextJsonMessageSerializer.Instance);

                if (message.DestinationAddress == null)
                {
                    LogContext.Warning?.Log("Outbox message DestinationAddress not present: {SequenceNumber} {MessageId}", message.SequenceNumber,
                        message.MessageId);
                    continue;
                }

                try
                {
                    using var sendToken = new CancellationTokenSource(_options.MessageDeliveryTimeout);
                    using var token = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, sendToken.Token);

                    var pipe = new OutboxMessageSendPipe(message, message.DestinationAddress);
                    var endpoint = await _busControl.GetSendEndpoint(message.DestinationAddress).ConfigureAwait(false);

                    StartedActivity? activity = LogContext.Current?.StartOutboxDeliverActivity(message);
                    StartedInstrument? instrument = LogContext.Current?.StartOutboxDeliveryInstrument(message);
                    try
                    {
                        await endpoint.Send(new SerializedMessageBody(), pipe, token.Token).ConfigureAwait(false);
                    }
                    catch (Exception exception)
                    {
                        activity?.AddExceptionEvent(exception);
                        instrument?.AddException(exception);
                        throw;
                    }
                    finally
                    {
                        activity?.Stop();
                        instrument?.Stop();
                    }

                    sentSequenceNumber = message.SequenceNumber;
                    await session.DeleteAsync(message, cancellationToken).ConfigureAwait(false);
                    deliveredCount++;

                    LogContext.Debug?.Log("Outbox Sent: {OutboxId} {SequenceNumber} {MessageId}", message.OutboxId, sentSequenceNumber, message.MessageId);
                }
                catch (Exception ex)
                {
                    LogContext.Warning?.Log(ex, "Outbox Send Fault: {OutboxId} {SequenceNumber} {MessageId}", message.OutboxId, message.SequenceNumber,
                        message.MessageId);
                    break;
                }
            }

            if (sentSequenceNumber > 0)
                outboxState.LastSequenceNumber = sentSequenceNumber;

            if (messageIndex == messages.Count && messages.Count < messageLimit)
            {
                outboxState.Delivered = DateTime.UtcNow;

                if (hasLastSequenceNumber == false)
                {
                    foreach (var message in messages)
                        await session.DeleteAsync(message, cancellationToken).ConfigureAwait(false);

                    await session.DeleteAsync(outboxState, cancellationToken).ConfigureAwait(false);
                }
                else
                    await session.UpdateAsync(outboxState, cancellationToken).ConfigureAwait(false);

                LogContext.Debug?.Log("Outbox Delivered: {OutboxId} {Delivered}", outboxState.OutboxId, outboxState.Delivered);
            }
            else
                await session.UpdateAsync(outboxState, cancellationToken).ConfigureAwait(false);

            return deliveredCount;
        }
    }
}

