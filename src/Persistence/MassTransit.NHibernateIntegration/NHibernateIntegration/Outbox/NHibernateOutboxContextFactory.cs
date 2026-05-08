#nullable enable
namespace MassTransit.NHibernateIntegration.Outbox
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Options;
    using Middleware;
    using Middleware.Outbox;
    using NHibernate;
    using NHibernate.Exceptions;


    public class NHibernateOutboxContextFactory :
        IOutboxContextFactory<ISessionFactory>
    {
        readonly IsolationLevel _isolationLevel;
        readonly IServiceProvider _provider;
        readonly INHibernateTenantSessionFactoryProvider _tenantSessionFactoryProvider;

        public NHibernateOutboxContextFactory(INHibernateTenantSessionFactoryProvider tenantSessionFactoryProvider, IServiceProvider provider,
            IOptions<NHibernateOutboxOptions> options)
        {
            _tenantSessionFactoryProvider = tenantSessionFactoryProvider;
            _provider = provider;
            _isolationLevel = options.Value.IsolationLevel;
        }

        public async Task Send<T>(ConsumeContext<T> context, OutboxConsumeOptions options, IPipe<OutboxConsumeContext<T>> next)
            where T : class
        {
            var messageId = context.GetOriginalMessageId() ?? throw new MessageException(typeof(T), "MessageId required to use the outbox");

            async Task<bool> Execute()
            {
                var lockId = NewId.NextGuid();

                var timer = Stopwatch.StartNew();

                var sessionFactory = _tenantSessionFactoryProvider.GetSessionFactory(_tenantSessionFactoryProvider.PartitionKey);
                using var session = sessionFactory.OpenSession();
                using var transaction = session.BeginTransaction(_isolationLevel);

                try
                {
                    InboxState? inboxState = await LoadAndLockInboxState(session, messageId, options.ConsumerId, context.CancellationToken)
                        .ConfigureAwait(false);

                    bool continueProcessing;

                    if (inboxState == null)
                    {
                        inboxState = new InboxState
                        {
                            Id = NewId.NextGuid(),
                            MessageId = messageId,
                            ConsumerId = options.ConsumerId,
                            Received = DateTime.UtcNow,
                            LockId = lockId,
                            ReceiveCount = 0
                        };

                        await session.SaveAsync(inboxState, context.CancellationToken).ConfigureAwait(false);
                        await session.FlushAsync(context.CancellationToken).ConfigureAwait(false);

                        continueProcessing = true;
                    }
                    else
                    {
                        inboxState.LockId = lockId;
                        inboxState.ReceiveCount++;

                        await session.UpdateAsync(inboxState, context.CancellationToken).ConfigureAwait(false);
                        await session.FlushAsync(context.CancellationToken).ConfigureAwait(false);

                        var outboxContext = new NHibernateOutboxConsumeContext<T>(context, options, _provider, session, inboxState);

                        await next.Send(outboxContext).ConfigureAwait(false);

                        try
                        {
                            await session.FlushAsync(context.CancellationToken).ConfigureAwait(false);
                        }
                        catch (Exception exception)
                        {
                            await context.NotifyFaulted(timer.Elapsed, TypeCache<T>.ShortName, exception).ConfigureAwait(false);
                            throw;
                        }

                        continueProcessing = outboxContext.ContinueProcessing;
                    }

                    try
                    {
                        await transaction.CommitAsync(context.CancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception exception)
                    {
                        await context.NotifyFaulted(timer.Elapsed, TypeCache<T>.ShortName, exception).ConfigureAwait(false);
                        throw;
                    }

                    return continueProcessing;
                }
                catch (Exception)
                {
                    await RollbackTransaction(transaction).ConfigureAwait(false);
                    throw;
                }
            }

            var continueProcessing = true;
            while (continueProcessing)
                continueProcessing = await Execute().ConfigureAwait(false);
        }

        public void Probe(ProbeContext context)
        {
            var scope = context.CreateFilterScope("outboxContextFactory");
            scope.Add("provider", "nhibernate");
        }

        static Task RollbackTransaction(ITransaction transaction)
        {
            try
            {
                return transaction.RollbackAsync(CancellationToken.None);
            }
            catch (Exception)
            {
                return Task.CompletedTask;
            }
        }

        static Task<InboxState?> LoadAndLockInboxState(ISession session, Guid messageId, Guid consumerId, CancellationToken cancellationToken)
        {
            // LockMode.Upgrade forces the row to be locked during the transaction for duplicate detection + in-order outbox delivery.
            return session.CreateQuery("from InboxState this where MessageId = :messageId and ConsumerId = :consumerId")
                .SetParameter("messageId", messageId)
                .SetParameter("consumerId", consumerId)
                .SetLockMode("this", LockMode.Upgrade)
                .UniqueResultAsync<InboxState>(cancellationToken)!;
        }
    }
}

