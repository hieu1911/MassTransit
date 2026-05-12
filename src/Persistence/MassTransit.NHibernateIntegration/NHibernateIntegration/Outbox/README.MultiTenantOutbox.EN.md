# NHibernate Transactional Outbox for Multi-Tenant Databases

This document describes the extension added to `MassTransit.NHibernateIntegration` to support the outbox pattern in a multi-tenant system where each tenant uses a separate database with the same schema.

## Goals

- Resolve the correct `ISessionFactory` per tenant (usually based on `connectionString`).
- Partition outbox notifications per tenant so `BusOutboxDeliveryService` only wakes up for the matching database.
- Keep MassTransit outbox usage unchanged, while replacing session/factory resolution logic.

## New Components

- `INHibernateTenantSessionFactoryProvider`
  - `PartitionKey`: the partition identifier (should map to tenant/connection string).
  - `GetSessionFactory()`: returns the tenant-specific `ISessionFactory` (based on request/company ambient context).
  - `GetSessionFactory(partitionKey)`: returns the `ISessionFactory` for a specific key (used by background multi-DB delivery).
- `INHibernateTenantDatabaseFactory`
  - `GetAllConnectionStrings()`: returns the list of tenant DBs that the delivery service must poll/deliver.
- `DefaultNHibernateTenantSessionFactoryProvider`
  - Fallback for single-tenant usage (`PartitionKey = "default"`).
- `ITenantBusOutboxNotification` + `TenantBusOutboxNotification`
  - Per-partition notification instead of one global notification channel.

## Runtime Flow

1. During publish/send in scope, `NHibernateScopedBusContext` opens the session using `INHibernateTenantSessionFactoryProvider.GetSessionFactory(partitionKey)`.
2. Outbox rows/messages are written to the correct tenant database.
3. After transaction commit, a notification is emitted with that tenant `PartitionKey`.
4. `BusOutboxDeliveryService` starts one worker loop per `connectionString/partitionKey` from `INHibernateTenantDatabaseFactory`, then waits by `_notification.WaitForDelivery(partitionKey)` and delivers from the matching DB.

## Usage

### 1) Register outbox as usual

```csharp
services.AddMassTransit(x =>
{
    x.AddNHibernateOutbox(o =>
    {
        o.UseBusOutbox();
    });
});
```

### 2) Override tenant session factory provider

Register a scoped implementation of `INHibernateTenantSessionFactoryProvider`.

```csharp
services.AddScoped<INHibernateTenantSessionFactoryProvider, FxTenantSessionFactoryProvider>();
```

## Example Integration with `Company.ConnectionString`

The example below maps well to the `ServiceFactory.Resolve<T>(company)` pattern in `fxcore`.

```csharp
public class FxTenantSessionFactoryProvider : INHibernateTenantSessionFactoryProvider
{
    readonly CompanyContext _companyContext;
    readonly ITenantSessionFactoryCache _sessionFactoryCache;

    public FxTenantSessionFactoryProvider(
        CompanyContext companyContext,
        ITenantSessionFactoryCache sessionFactoryCache)
    {
        _companyContext = companyContext;
        _sessionFactoryCache = sessionFactoryCache;
    }

    public string PartitionKey
    {
        get
        {
            var company = _companyContext.CurrentCompany;
            var connectionString = company.Config["ConnectionString"];
            return connectionString.Trim();
        }
    }

    public ISessionFactory GetSessionFactory()
    {
        return _sessionFactoryCache.GetOrCreate(PartitionKey);
    }

    public ISessionFactory GetSessionFactory(string partitionKey)
    {
        // Background delivery uses partitionKey directly (usually connectionString).
        return _sessionFactoryCache.GetOrCreate(partitionKey);
    }
}
```

If your application resolves transaction services like:

```csharp
ServiceFactory.Resolve<ITransactionService>(company)
```

make sure the current `company` is set in `CompanyContext` before publish/send, so outbox always targets the correct tenant database.

## Publish Example with Transaction Open/Close

The example below shows a common `fxcore` flow: resolve services by `company`, open a transaction, write domain data + publish an event, then commit/rollback and close the session.

```csharp
public class InvoiceApplicationService
{
    readonly IPublishEndpoint _publishEndpoint;

    public InvoiceApplicationService(IPublishEndpoint publishEndpoint)
    {
        _publishEndpoint = publishEndpoint;
    }

    public async Task CreateInvoiceAsync(Company company, Invoice invoice, CancellationToken cancellationToken)
    {
        var transactionService = ServiceFactory.Resolve<ITransactionService>(company);

        try
        {
            transactionService.BeginTran();

            // 1) Persist domain data on tenant DB
            transactionService.InvoiceService.CreateNew(invoice);

            // 2) Publish integration event in the same NHibernate transaction
            await _publishEndpoint.Publish(new InvoiceCreated
            {
                InvoiceId = invoice.Id,
                CompanyId = company.Id,
                Amount = invoice.TotalAmount
            }, cancellationToken);

            // 3) Commit DB transaction.
            // MassTransit NHibernate outbox marks outbox rows as ready after commit.
            transactionService.CommitTran();
        }
        catch
        {
            transactionService.RolbackTran();
            throw;
        }
        finally
        {
            // Always close the bound NHibernate session for current tenant/connection string
            transactionService.UnbindSession();
        }
    }
}
```

In this flow:

- If the transaction rolls back, the message is not delivered to the broker.
- If commit succeeds, `BusOutboxDeliveryService` for the matching tenant partition delivers the message asynchronously.

## Implementation Notes

- Use a stable `PartitionKey` (normalized connection string or hash).
- If you use a hash as `PartitionKey`, still cache `ISessionFactory` by the original connection string.
- `ISessionFactory` is expensive; cache and reuse it instead of rebuilding per request.

## Integration with Existing Transaction Services (Important)

When the application already uses `service.BeginTran()/CommitTran()/RolbackTran()` (for example, an `InvoiceService` pattern), transactional outbox works correctly only if:

- `Publish/Send` uses the **same `ISession` and the same `ITransaction`** as the current service transaction.
- You do not create a temporary publish scope that can own and dispose the currently bound `ISession` (a common cause of `Session is closed`).
- Outbox opens a new session/transaction only when no ambient session is available.

### Expected Behavior

- `CommitTran` succeeds -> `OutboxState/OutboxMessage` is persisted, then delivery service publishes to the queue.
- `RolbackTran` (or exception before commit) -> outbox rows are rolled back with the transaction, so **no message is published**.

### Quick Debug Checklist

1. Right after `Publish`, check whether rows exist in `OutboxState/OutboxMessage`.
2. If no outbox rows exist: publish did not go through bus outbox pipeline (or publish errors were swallowed).
3. If outbox rows exist but nothing reaches the queue: verify delivery worker and `PartitionKey` notification flow.
4. If `Session is closed` appears: re-check `ISession` ownership/lifecycle in the publish path.

