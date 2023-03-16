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

        if (kafkaServiceConfiguration.IsUsingAuthentication.Value)
        {
            if (Enum.TryParse(kafkaServiceConfiguration.SaslMechanism, true, out SaslMechanism mechanismValue))
            {
                this._producerConfig.SaslMechanism = mechanismValue;
            }

            if (Enum.TryParse(kafkaServiceConfiguration.SecurityProtocol, true,
                    out SecurityProtocol protocolValue))
            {
                this._producerConfig.SecurityProtocol = protocolValue;
            }

            this._producerConfig.SaslUsername = kafkaServiceConfiguration.Username;
            this._producerConfig.SaslPassword = kafkaServiceConfiguration.Password;
        }

        this._producerConfig = new ProducerConfig(new ClientConfig()
        {
            BootstrapServers = $"{kafkaServiceConfiguration.Server}:{kafkaServiceConfiguration.Port}"
        });
            
            if (kafkaServiceConfiguration.IsUsingAuthentication.Value)
            {
                if (Enum.TryParse(kafkaServiceConfiguration.SaslMechanism, true, out SaslMechanism mechanismValue))
                {
                    this._consumerConfig.SaslMechanism = mechanismValue;
                }

                if (Enum.TryParse(kafkaServiceConfiguration.SecurityProtocol, true,
                        out SecurityProtocol protocolValue))
                {
                    this._consumerConfig.SecurityProtocol = protocolValue;
                }

                this._consumerConfig.SaslUsername = kafkaServiceConfiguration.Username;
                this._consumerConfig.SaslPassword = kafkaServiceConfiguration.Password;
            }

            this._consumerConfig = new ConsumerConfig(new ClientConfig()
            {
                BootstrapServers = $"{kafkaServiceConfiguration.Server}:{kafkaServiceConfiguration.Port}"
            })
            {
                GroupId = $"{kafkaServiceConfiguration.ConsumerGroupId}",
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };
        

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
            await producer.ProduceAsync(topic: eventName
                , new Message<Null, string>()
                {
                    Value = @serializedValue
                }
            );
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "Error while publishing: {message}", exception.Message);
            throw;
        }
    }

    public async Task SubscribeAsync<TEventType, TEventHandlerType>(CancellationToken cancellationToken) 
        where TEventType : IEvent 
        where TEventHandlerType : class, IEventHandler<TEventType>
    {
        using IConsumer<Null, string> consumer = this._consumerBuilder.Build();
        string eventName = typeof(TEventType).Name;

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
                        using IServiceScope scope = this._serviceScopeFactory.CreateScope();
                        TEventHandlerType eventHandler = scope.ServiceProvider.GetRequiredService<TEventHandlerType>();
                        TEventType eventObj = JsonSerializer.Deserialize<TEventType>(consumeResult.Message.Value);
                        await eventHandler.HandleEvent(eventObj, cancellationToken);
                    }
                }
                catch (JsonException jsonException)
                {
                    _logger.LogError(jsonException,
                        "Error while subscribing: {Message} \n Json: {ConsumeResultMessage}", 
                        jsonException.Message,
                        consumeResult.Message.Value);
                }
                catch (Exception exception)
                {
                    _logger.LogError(exception, "Error while subscribing: {Message}", exception.Message);
                }
            } while (true);
        }, cancellationToken).ConfigureAwait(false);
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
            _logger.LogWarning(createTopicsException, "{message}", createTopicsException.Message);
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "Error creating topic. {message}", exception.Message);
            throw;
        }
    }
}