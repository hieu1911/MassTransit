#nullable enable
namespace MassTransit.NHibernateIntegration.Outbox
{
    using System;


    public class InboxState
    {
        public virtual Guid Id { get; set; }

        public virtual Guid MessageId { get; set; }

        public virtual Guid ConsumerId { get; set; }

        public virtual Guid LockId { get; set; }

        public virtual int Version { get; set; }

        public virtual DateTime Received { get; set; }

        public virtual int ReceiveCount { get; set; }

        public virtual DateTime? ExpirationTime { get; set; }

        public virtual DateTime? Consumed { get; set; }

        public virtual DateTime? Delivered { get; set; }

        public virtual long? LastSequenceNumber { get; set; }
    }
}

