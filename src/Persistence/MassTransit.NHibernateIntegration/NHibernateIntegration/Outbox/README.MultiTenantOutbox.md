# NHibernate Transactional Outbox for Multi-Tenant DB

Tài liệu này mô tả phần mở rộng vừa thêm cho `MassTransit.NHibernateIntegration` để hỗ trợ outbox khi hệ thống có nhiều tenant, mỗi tenant dùng một database riêng (cùng schema).

## Mục tiêu

- Chọn đúng `ISessionFactory` theo tenant (thường dựa trên `connectionString`).
- Partition notification theo tenant để `BusOutboxDeliveryService` chỉ wake-up đúng DB cần delivery.
- Giữ API outbox hiện tại của MassTransit, chỉ thay phần resolve session/factory.

## Thành phần mới
- `INHibernateTenantSessionFactoryProvider`
  - `PartitionKey`: key định danh partition (nên map theo tenant/connection string).
  - `GetSessionFactory()`: trả về `ISessionFactory` đúng tenant hiện tại (theo request/company ambient).
  - `GetSessionFactory(partitionKey)`: trả về `ISessionFactory` đúng tenant theo key do host truyền vào (dùng cho background delivery multi-DB).
- `INHibernateTenantDatabaseFactory`
  - `GetAllConnectionStrings()`: trả về danh sách các tenant DB mà delivery service cần poll/deliver.
- `DefaultNHibernateTenantSessionFactoryProvider`
  - Fallback cho trường hợp single-tenant (`PartitionKey = "default"`).
- `ITenantBusOutboxNotification` + `TenantBusOutboxNotification`
  - Notification theo partition key thay vì global notification.

## Luồng hoạt động

1. Khi publish/send trong scope, `NHibernateScopedBusContext` mở session bằng `INHibernateTenantSessionFactoryProvider.GetSessionFactory(partitionKey)`.
2. Outbox row/message được ghi vào đúng DB tenant.
3. Khi transaction commit, notification được gửi với `PartitionKey` của tenant đó.
4. `BusOutboxDeliveryService` khởi tạo 1 worker loop cho mỗi `connectionString/partitionKey` từ `INHibernateTenantDatabaseFactory`, sau đó chờ `_notification.WaitForDelivery(partitionKey)` và đọc/deliver từ đúng DB.

## Cách dùng

### 1) Đăng ký outbox như cũ

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

Đăng ký implementation `Scoped` của `INHibernateTenantSessionFactoryProvider`.

```csharp
services.AddScoped<INHibernateTenantSessionFactoryProvider, FxTenantSessionFactoryProvider>();
```

`FxTenantSessionFactoryProvider` cần implement thêm overload `GetSessionFactory(partitionKey)` để background delivery có thể mở DB theo key.

## Ví dụ tích hợp theo `Company.ConnectionString`

Ví dụ dưới đây minh họa cách map với pattern `ServiceFactory.Resolve<T>(company)` trong `fxcore`.

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

Nếu hệ thống của bạn resolve service theo:

```csharp
ServiceFactory.Resolve<ITransactionService>(company)
```

thì nên đảm bảo `company` hiện tại đã được set vào `CompanyContext` trước khi publish/send, để outbox luôn chọn đúng DB tenant.

## Ví dụ publish message với mở/đóng transaction

Ví dụ dưới đây minh họa một flow phổ biến trong `fxcore`: resolve service theo `company`, mở transaction, thay đổi dữ liệu + publish event, rồi commit/rollback và đóng session.

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
            // MassTransit NHibernate outbox will mark the outbox row as ready after commit.
            transactionService.CommitTran();
        }
        catch
        {
            transactionService.RolbackTran();
            throw;
        }
        finally
        {
            // Always close bound NHibernate session for current tenant/connection string
            transactionService.UnbindSession();
        }
    }
}
```

Trong ví dụ này:

- Nếu transaction rollback, message không được deliver ra broker.
- Nếu commit thành công, `BusOutboxDeliveryService` của đúng tenant partition sẽ deliver message bất đồng bộ.

## Lưu ý triển khai

- Nên dùng key ổn định cho `PartitionKey` (connection string normalize hoặc hash).
- Nếu dùng hash làm partition key, hãy vẫn cache `ISessionFactory` theo connection string gốc.
- `ISessionFactory` là đối tượng nặng, cần cache/reuse thay vì build mới mỗi request.

