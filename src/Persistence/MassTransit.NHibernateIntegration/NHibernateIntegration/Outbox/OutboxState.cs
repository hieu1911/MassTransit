#nullable enable
namespace MassTransit.NHibernateIntegration.Outbox
{
    using System;


    public class OutboxState
    {
        public virtual Guid OutboxId { get; set; }

        public virtual Guid LockId { get; set; }

        public virtual int Version { get; set; }

        public virtual DateTime Created { get; set; }

        public virtual DateTime? Delivered { get; set; }

        public virtual long? LastSequenceNumber { get; set; }
    }
}

