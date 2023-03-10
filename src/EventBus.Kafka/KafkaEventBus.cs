using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FonRadar.Base.EventBus.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace FonRadar.Base.EventBus.Kafka;

public class KafkaEventBus : IKafkaEventBus
{
    private readonly ILogger<IEventBus> _logger;
    private readonly ISubscriptionManager _eventBusSubscriptionManager;
    private readonly IServiceScopeFactory _serviceScopeFactory;
    private readonly KafkaServiceConfiguration _kafkaServiceConfiguration;

    private readonly ProducerConfig _producerConfig;
    private readonly ConsumerConfig _consumerConfig;
    private readonly ConsumerBuilder<Null, string> _consumerBuilder;
   
    public KafkaEventBus(
        ILogger<IEventBus> logger
        , ISubscriptionManager eventBusSubscriptionManager
        , IServiceScopeFactory serviceScopeFactory
        , KafkaServiceConfiguration kafkaServiceConfiguration
        )
    {
        this._logger = logger;
        this._eventBusSubscriptionManager = eventBusSubscriptionManager;
        this._serviceScopeFactory = serviceScopeFactory;
        this._kafkaServiceConfiguration = kafkaServiceConfiguration;

        this._producerConfig = new ProducerConfig( new ClientConfig()
        {
            BootstrapServers = $"{this._kafkaServiceConfiguration.Server}:{this._kafkaServiceConfiguration.Port}"
        });
        
        if (this._kafkaServiceConfiguration.IsUsingAuthentication)
        {
            if (Enum.TryParse(_kafkaServiceConfiguration.SaslMechanism, true, out SaslMechanism mechanismValue))
            {
                this._producerConfig.SaslMechanism = mechanismValue;
            }
            
            if (Enum.TryParse(_kafkaServiceConfiguration.SecurityProtocol, true, out SecurityProtocol protocolValue))
            {
                this._producerConfig.SecurityProtocol = protocolValue;
            }
            
            this._producerConfig.SaslUsername = _kafkaServiceConfiguration.Username;
            this._producerConfig.SaslPassword = _kafkaServiceConfiguration.Password;
        }

        this._consumerConfig = new ConsumerConfig( new ClientConfig()
        {
            BootstrapServers = $"{this._kafkaServiceConfiguration.Server}:{this._kafkaServiceConfiguration.Port}"
        })
        {
            GroupId = $"{this._kafkaServiceConfiguration.ConsumerGroupId}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };
        
        if (this._kafkaServiceConfiguration.IsUsingAuthentication)
        {
            if (Enum.TryParse(_kafkaServiceConfiguration.SaslMechanism, true, out SaslMechanism mechanismValue))
            {
                this._consumerConfig.SaslMechanism = mechanismValue;
            }

            if (Enum.TryParse(_kafkaServiceConfiguration.SecurityProtocol, true, out SecurityProtocol protocolValue))
            {
                this._consumerConfig.SecurityProtocol = protocolValue;
            }

            this._consumerConfig.SaslUsername = _kafkaServiceConfiguration.Username;
            this._consumerConfig.SaslPassword = _kafkaServiceConfiguration.Password;
        }

        this._consumerBuilder = new ConsumerBuilder<Null, string>(this._consumerConfig);
    }

    public async Task PublishAsync<TEventType>(TEventType @event) where TEventType : IEvent
    {
        try
        {
            string eventName = typeof(TEventType).Name;
            IProducer<Null, string> producer = new ProducerBuilder<Null, string>(this._producerConfig).Build();
            await this.CreateTopicAsync(eventName);
            string serializedValue = JsonSerializer.Serialize(@event);
            DeliveryResult<Null, string> deliveryResult = await producer.ProduceAsync(topic: eventName
                , new Message<Null, string>()
                {
                    Value = @serializedValue
                }
            );
        }
        catch (Exception exception)
        {
            Console.WriteLine(exception);
            throw;
        }
    }

    public async Task SubscribeAsync<TEventType, TEventHandlerType>(CancellationToken cancellationToken) 
        where TEventType : IEvent 
        where TEventHandlerType : class, IEventHandler<TEventType>
    {
        using (IConsumer<Null, string> consumer = this._consumerBuilder.Build())
        {
            string eventName = typeof(TEventType).Name;
            string eventHandlerName = typeof(TEventHandlerType).Name;

            this._eventBusSubscriptionManager.Subscribe<TEventType, TEventHandlerType>();
            await this.CreateTopicAsync(eventName);
            consumer.Subscribe(topic:eventName);
            // TODO @salih => buradaki looptask'ın yaşam döngüsü ve diğer handler'ın da call edilmesi için geniş bir zamanda test edilmesi gerekiyor
            await Task.Run(async () =>
            {
                do
                {
                    ConsumeResult<Null, string>? consumeResult = null;
                    try
                    {
                        consumeResult = consumer.Consume();
                        if (this._eventBusSubscriptionManager.HasEvent<TEventType>())
                        {
                            using (IServiceScope scope = this._serviceScopeFactory.CreateScope())
                            {
                                TEventHandlerType eventHandler = scope.ServiceProvider.GetRequiredService<TEventHandlerType>();
                                TEventType eventObj = JsonSerializer.Deserialize<TEventType>(consumeResult.Message.Value);
                                await eventHandler.HandleEvent(eventObj, cancellationToken);
                            }
                        }
                    }
                    catch (JsonException jsonException)
                    {
                        Console.WriteLine(consumeResult.Message.Value);
                    }
                    catch (Exception exception)
                    {
                        Console.WriteLine(exception);
                    }
                } while (true);
                
            }, cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task CreateTopicAsync(string eventName)
    {
        try
        {
            IAdminClient adminClient = new AdminClientBuilder(this._producerConfig).Build();
            TopicSpecification topicSpecification = new TopicSpecification();
            topicSpecification.Name = eventName;
            await adminClient.CreateTopicsAsync(new[] { topicSpecification });
        }
        catch (CreateTopicsException createTopicsException)
        {
            Console.WriteLine(createTopicsException);
        }
        catch (Exception exception)
        {
            Console.WriteLine(exception);
            throw;
        }
    }
}