# Transactional Outbox for NHibernate Integration

This document summarizes the Transactional Outbox support added to `MassTransit.NHibernateIntegration`, based on the same overall approach used by `MassTransit.EntityFrameworkCoreIntegration`.

## Goals

- Support the Inbox/Outbox pattern for NHibernate-based consumer pipelines.
- Ensure idempotent processing using `MessageId + ConsumerId`.
- Store `Send/Publish` operations in the outbox within the same transaction as message consumption.
- Periodically clean up delivered inbox records after the duplicate detection window expires.

## Added Files

### Outbox Configuration

- `Configuration/INHibernateOutboxConfigurator.cs`
- `Configuration/INHibernateBusOutboxConfigurator.cs`
- `Configuration/Configuration/NHibernateOutboxConfigurator.cs`
- `Configuration/Configuration/NHibernateBusOutboxConfigurator.cs`
- `Configuration/NHibernateOutboxConfigurationExtensions.cs`

### Outbox Runtime

- `NHibernateIntegration/Outbox/NHibernateOutboxOptions.cs`
- `NHibernateIntegration/Outbox/NHibernateOutboxContextFactory.cs`
- `NHibernateIntegration/Outbox/NHibernateOutboxConsumeContext.cs`
- `NHibernateIntegration/Outbox/NHibernateScopedBusContext.cs`
- `NHibernateIntegration/Outbox/NHibernateConsumeContextScopedBusContext.cs`
- `NHibernateIntegration/Outbox/NHibernateScopedBusContextProvider.cs`
- `NHibernateIntegration/Outbox/BusOutboxDeliveryService.cs`
- `NHibernateIntegration/Outbox/InboxCleanupService.cs`
- `NHibernateIntegration/Outbox/NHibernateOutboxExtensions.cs`

### Outbox State Model and Mappings

- `NHibernateIntegration/Outbox/InboxState.cs`
- `NHibernateIntegration/Outbox/OutboxState.cs`
- `NHibernateIntegration/Outbox/OutboxMessage.cs`
- `NHibernateIntegration/Outbox/InboxStateMap.cs`
- `NHibernateIntegration/Outbox/OutboxStateMap.cs`
- `NHibernateIntegration/Outbox/OutboxMessageMap.cs`
- `NHibernateIntegration/Outbox/NHibernateOutboxMappings.cs`

## New APIs

### Register Outbox in DI

```csharp
x.AddNHibernateOutbox(o =>
{
    o.IsolationLevel = System.Data.IsolationLevel.RepeatableRead;
    // o.DisableInboxCleanupService();
    o.UseBusOutbox();
});
```

### Apply Transactional Outbox on a Receive Endpoint

```csharp
e.UseNHibernateOutbox(context);
```

Legacy overload is also available:

```csharp
e.UseNHibernateOutbox(provider);
```

## Full Usage

### 1) Add Outbox Mappings to NHibernate Model

When creating `ISessionFactory`, include outbox mapping types:

```csharp
var mappedTypes = new[]
{
    // ... your saga mappings
}.Concat(MassTransit.NHibernateIntegration.Outbox.NHibernateOutboxMappings.Mappings);
```

Or pass `NHibernateOutboxMappings.Mappings` directly to your session factory provider.

### 2) Register `ISessionFactory` in DI

Ensure `ISessionFactory` is registered in the container so the outbox context factory can be resolved.

### 3) Configure MassTransit

```csharp
services.AddMassTransit(x =>
{
    x.AddNHibernateOutbox(o =>
    {
        o.IsolationLevel = System.Data.IsolationLevel.RepeatableRead;
        o.DuplicateDetectionWindow = TimeSpan.FromMinutes(30);
        o.QueryDelay = TimeSpan.FromSeconds(10);
        o.QueryMessageLimit = 100;
        o.QueryTimeout = TimeSpan.FromSeconds(30);
    });

    x.UsingRabbitMq((context, cfg) =>
    {
        cfg.ReceiveEndpoint("example-queue", e =>
        {
            e.UseNHibernateOutbox(context);
            e.ConfigureConsumers(context);
        });
    });
});
```

## Important Notes

- This implementation focuses on consumer transactional outbox (inbox + outbox in the consume pipeline).
- It includes bus outbox support via `UseBusOutbox(...)`, which intercepts `IPublishEndpoint` and `ISendEndpointProvider`
  and delivers persisted outbox messages using a hosted background service.
- Duplicate detection locking uses pessimistic row locking (`LockMode.Upgrade`) on `InboxState`.
- You should add appropriate indexes/constraints in your database schema for performance and duplicate prevention.

## Verification

The changes were validated with successful builds of:

- `MassTransit.NHibernateIntegration.csproj`
- `MassTransit.sln`

# Transactional Outbox for NHibernate Integration

Tai lieu nay tong hop cac thay doi da duoc them de ho tro Transactional Outbox trong `MassTransit.NHibernateIntegration`, dua theo mo hinh cua `MassTransit.EntityFrameworkCoreIntegration`.

## Muc tieu

- Ho tro Inbox/Outbox pattern cho consumer pipeline su dung NHibernate.
- Dam bao xu ly idempotent theo `MessageId + ConsumerId`.
- Luu cac lenh `Send/Publish` vao outbox trong cung transaction voi xu ly message.
- Dinh ky cleanup inbox records da delivered het han duplicate detection window.

## Cac file da them

### Cau hinh Outbox

- `Configuration/INHibernateOutboxConfigurator.cs`
- `Configuration/Configuration/NHibernateOutboxConfigurator.cs`
- `Configuration/NHibernateOutboxConfigurationExtensions.cs`

### Runtime Outbox

- `NHibernateIntegration/Outbox/NHibernateOutboxOptions.cs`
- `NHibernateIntegration/Outbox/NHibernateOutboxContextFactory.cs`
- `NHibernateIntegration/Outbox/NHibernateOutboxConsumeContext.cs`
- `NHibernateIntegration/Outbox/InboxCleanupService.cs`
- `NHibernateIntegration/Outbox/NHibernateOutboxExtensions.cs`

### Outbox state model + mappings

- `NHibernateIntegration/Outbox/InboxState.cs`
- `NHibernateIntegration/Outbox/OutboxState.cs`
- `NHibernateIntegration/Outbox/OutboxMessage.cs`
- `NHibernateIntegration/Outbox/InboxStateMap.cs`
- `NHibernateIntegration/Outbox/OutboxStateMap.cs`
- `NHibernateIntegration/Outbox/OutboxMessageMap.cs`
- `NHibernateIntegration/Outbox/NHibernateOutboxMappings.cs`

## Cac API moi

### Dang ky outbox trong DI

```csharp
x.AddNHibernateOutbox(o =>
{
    o.IsolationLevel = System.Data.IsolationLevel.RepeatableRead;
    // o.DisableInboxCleanupService();
});
```

### Gan transactional outbox vao receive endpoint

```csharp
e.UseNHibernateOutbox(context);
```

Co ho tro ca overload cu:

```csharp
e.UseNHibernateOutbox(provider);
```

## Cach su dung day du

### 1) Add outbox mappings vao NHibernate model

Khi tao `ISessionFactory`, can include cac map cua outbox:

```csharp
var mappedTypes = new[]
{
    // ... saga mappings cua ban
}.Concat(MassTransit.NHibernateIntegration.Outbox.NHibernateOutboxMappings.Mappings);
```

Hoac truyen truc tiep `NHibernateOutboxMappings.Mappings` vao provider.

### 2) Dang ky `ISessionFactory` vao DI

Dam bao `ISessionFactory` co trong container de outbox context factory co the resolve.

### 3) Cau hinh MassTransit

```csharp
services.AddMassTransit(x =>
{
    x.AddNHibernateOutbox(o =>
    {
        o.IsolationLevel = System.Data.IsolationLevel.RepeatableRead;
        o.DuplicateDetectionWindow = TimeSpan.FromMinutes(30);
        o.QueryDelay = TimeSpan.FromSeconds(10);
        o.QueryMessageLimit = 100;
        o.QueryTimeout = TimeSpan.FromSeconds(30);
    });

    x.UsingRabbitMq((context, cfg) =>
    {
        cfg.ReceiveEndpoint("example-queue", e =>
        {
            e.UseNHibernateOutbox(context);
            e.ConfigureConsumers(context);
        });
    });
});
```

## Luu y quan trong

- Outbox nay tap trung vao consumer transactional outbox (inbox + outbox in consume pipeline).
- Chua bao gom bus outbox delivery service tuong duong `UseBusOutbox(...)` cua EFCore.
- De lock duplicate detection, implementation su dung pessimistic row lock (`LockMode.Upgrade`) tren `InboxState`.
- Nen tao index/constraint phu hop cho bang outbox trong DB de toi uu truy van va tranh duplicate.

## Kiem tra

Thay doi da duoc xac nhan build thanh cong:

- `MassTransit.NHibernateIntegration.csproj`
- `MassTransit.sln`

