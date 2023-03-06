# EventBus.Kafka
<b>EventBus.Kafka</b> is a <b>Kafka</b> implementation of the <a href="https://github.com/FonRadar/EventBus">Fon Radar Event Bus</a> <br>
This nuget package provides a quick and clean solution to messaging with <b>Kafka</b> and currently supports only .NET 6 and .NET 7

## Quick Start
To installing a package run command or install from nuget store.
```
dotnet add package FonRadar.Base.EventBus.Kafka
```
<br>
Register your event bus like this

```csharp
builder.Services
    .AddEventBus()
    .AddKafkaEventBus(myKafkaConfiguration =>
    {
        myKafkaConfiguration.Server = "myKafkaServer";
        myKafkaConfiguration.Port = "myPort";
        //And other fields you desire
    });
```
or like this
```csharp
builder.Services
    .AddEventBus()
    .AddKafkaEventBus("appsettings section goes here!");
```

An example appsettings section without authentication
```json
{
    "MyKafkaConfiguration": {
        "Server": "localhost",
        "Port": 9092,
        "ConsumerGroupId": "kafka-consumers",
        "IsUsingAuthentication" : false,
        "Username": "these",
        "Password": "values",
        "SaslMechanism" : "are",
        "SecurityProtocol" : "optional"
    }
}
```

<br>
An example appsettings section with authentication

```json
{
    "MyKafkaConfiguration": {
        "Server": "localhost",
        "Port": 9092,
        "ConsumerGroupId": "kafka-consumers",
        "IsUsingAuthentication" : true,
        "Username": "admin",
        "Password": "admin-secret",
        "SaslMechanism" : "PLAIN",
        "SecurityProtocol" : "Plaintext"
    }
}
```
> **Note**
> <br>
> Supported Sasl mechanisms values are GSSAPI, PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER <br>
> Supported Security Protocol values are Plaintext, Ssl, SaslPlaintext, SaslSsl
<br>
These registration types are also supporting generic arguments
<br> <br>

```csharp
public interface INotificationServiceEventBus : IKafkaEventBus
{
}
```
<br>

```csharp
public class NotificationServiceEventBus : KafkaEventBus, INotificationServiceEventBus
{
    public NotificationServiceEventBus(ILogger<IEventBus> logger, ISubscriptionManager eventBusSubscriptionManager, IServiceScopeFactory serviceScopeFactory, KafkaServiceConfiguration kafkaServiceConfiguration) 
        : base(logger, eventBusSubscriptionManager, serviceScopeFactory, kafkaServiceConfiguration)
    {
    }
}
```

<br>

```csharp
builder.Services
    .AddEventBus()
    .AddKafkaEventBus<INotificationServiceEventBus, NotificationServiceEventBus>("Integrations:NotificationService:Kafka");
```

<br>
In order to messaging with <b>Kafka</b> objects should be inherited from <b>Event</b> class
<br>

```csharp
public record DummyEvent : Event
{
    public string DummyMessage { get; set; }
}
```
> **Warning**
> <br>
> <b>Curently Kafka topic names are created from class names, make sure producer and consumer have the same class name in both projects</b>

<br>

### Producer

For producer, once event bus has registered, it can be simply published 

```csharp
    private readonly IKafkaEventBus _eventBus; //This can be another implementation which inherits IKafkaEventBus

    public EventPublisherProvider(
        IKafkaEventBus eventBus
    )
    {
        this._eventBus = eventBus;
    }

    public async Task PublishDummy(DummyEvent @event)
    {
        await this._eventBus.PublishAsync(@event);
    }
```

### Consumer

Consumer side must contain an event handler that inherits IEventHandler for every event
  
```csharp
public class DummyEventHandler : IEventHandler<DummyEvent>
{
    public Task HandleEvent(DummyEvent @event, CancellationToken cancellationToken)
    {
        //logic here 
    }
}
```

On service registration part event handlers must be registered 
```csharp
builder.Services.AddScoped<DummyEventHandler>();
```
> **Note**
> Currently this registration step is manual, further improvements will focus on this situtation

<br>
Consumer must subscribe every event that needs to processed 

```csharp
WebApplication app = builder.Build();
IKafkaEventBus eventBus = app.Services.GetRequiredService<IKafkaEventBus>();
eventBus.SubscribeAsync<DummyEvent, DummyEventHandler>(CancellationToken.None).ConfigureAwait(false);
```
<br> 

Further improvements are on the way! 👨‍💻 <br>
Feel free to share your thoughts or contribute to our project
