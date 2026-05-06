#nullable enable
namespace MassTransit.NHibernateIntegration.Outbox
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Middleware;
    using Middleware.Outbox;
    using NHibernate;


    public class NHibernateOutboxConsumeContext<TMessage> :
        OutboxConsumeContextProxy<TMessage>
        where TMessage : class
    {
        readonly InboxState _inboxState;
        readonly ISession _session;

        public NHibernateOutboxConsumeContext(ConsumeContext<TMessage> context, OutboxConsumeOptions options, IServiceProvider provider, ISession session,
            InboxState inboxState)
            : base(context, options, provider)
        {
            _session = session;
            _inboxState = inboxState;
        }

        public override Guid? MessageId => _inboxState.MessageId;

        public override bool ContinueProcessing { get; set; } = true;

        public override bool IsMessageConsumed => _inboxState.Consumed.HasValue;
        public override bool IsOutboxDelivered => _inboxState.Delivered.HasValue;
        public override int ReceiveCount => _inboxState.ReceiveCount;
        public override long? LastSequenceNumber => _inboxState.LastSequenceNumber;

        public override async Task SetConsumed()
        {
            _inboxState.Consumed = DateTime.UtcNow;
            await _session.UpdateAsync(_inboxState, CancellationToken).ConfigureAwait(false);
            await _session.FlushAsync(CancellationToken).ConfigureAwait(false);

            LogContext.Debug?.Log("Outbox Consumed: {MessageId} {Consumed}", MessageId, _inboxState.Consumed);
        }

        public override async Task SetDelivered()
        {
            _inboxState.Delivered = DateTime.UtcNow;
            await _session.UpdateAsync(_inboxState, CancellationToken).ConfigureAwait(false);
            await _session.FlushAsync(CancellationToken).ConfigureAwait(false);

            LogContext.Debug?.Log("Outbox Delivered: {MessageId} {Delivered}", MessageId, _inboxState.Delivered);
        }

        public override async Task<List<OutboxMessageContext>> LoadOutboxMessages()
        {
            var lastSequenceNumber = LastSequenceNumber ?? 0;

            IList<OutboxMessage> messages = await _session.QueryOver<OutboxMessage>()
                .Where(x => x.InboxMessageId == MessageId && x.InboxConsumerId == ConsumerId && x.SequenceNumber > lastSequenceNumber)
                .OrderBy(x => x.SequenceNumber).Asc
                .Take(Options.MessageDeliveryLimit + 1)
                .ListAsync(CancellationToken)
                .ConfigureAwait(false);

            foreach (var message in messages)
                message.Deserialize(SerializerContext);

            return messages.Cast<OutboxMessageContext>().ToList();
        }

        public override Task NotifyOutboxMessageDelivered(OutboxMessageContext message)
        {
            _inboxState.LastSequenceNumber = message.SequenceNumber;
            return _session.UpdateAsync(_inboxState, CancellationToken);
        }

        public override async Task RemoveOutboxMessages()
        {
            var count = await _session.CreateQuery("delete from OutboxMessage where InboxMessageId = :messageId and InboxConsumerId = :consumerId")
                .SetParameter("messageId", MessageId)
                .SetParameter("consumerId", ConsumerId)
                .ExecuteUpdateAsync(CancellationToken)
                .ConfigureAwait(false);

            if (count > 0)
                LogContext.Debug?.Log("Outbox removed {Count} messages: {MessageId}", count, MessageId);
        }

        public override Task AddSend<T>(SendContext<T> context)
            where T : class
        {
            return _session.AddSend(context, SerializerContext, MessageId, ConsumerId);
        }
    }
}

