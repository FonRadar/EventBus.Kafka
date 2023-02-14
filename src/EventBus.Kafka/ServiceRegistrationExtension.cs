using System;
using FonRadar.Base.EventBus.Internal;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace FonRadar.Base.EventBus.Kafka;

public static class ServiceRegistrationExtension
{
    public static IEventBusServiceRegistration AddKafkaEventBus<TEventBusType>(
        this IEventBusServiceRegistration serviceCollection
        , Action<KafkaServiceConfiguration> configure
        )
        where TEventBusType : class, IEventBus
    {
        serviceCollection.AddSingleton<TEventBusType>(provider =>
        {
            ILogger<IEventBus> logger = provider.GetRequiredService<ILogger<IEventBus>>();
            IServiceScopeFactory serviceScopeFactory = provider.GetRequiredService<IServiceScopeFactory>();
            ISubscriptionManager? subscriptionManager = provider.GetService<ISubscriptionManager>();
            if (subscriptionManager == null)
            {
                throw new OptionsValidationException("IEventBusSubscriptionManager", typeof(ISubscriptionManager),
                    new[] { "Please register first the `IEventBusSubscriptionManager` with using `service.AddEventBus()` method!" });
            }
            
            KafkaServiceConfiguration kafkaConfiguration = new KafkaServiceConfiguration();
            configure(kafkaConfiguration);
            if (string.IsNullOrEmpty(kafkaConfiguration.Server))
            {
                throw new OptionsValidationException("Server", typeof(string),
                    new[] { "Kafka Server is not defined!" });
            }
            
            if (string.IsNullOrEmpty(kafkaConfiguration.Port))
            {
                throw new OptionsValidationException("Port", typeof(string),
                    new[] { "Kafka Port is not defined!" });
            }
            
            if (string.IsNullOrEmpty(kafkaConfiguration.ConsumerGroupId))
            {
                throw new OptionsValidationException("ConsumerGroupId", typeof(string),
                    new[] { "Kafka ConsumerGroupId is not defined!" });
            }
            
            TEventBusType? eventBus = (TEventBusType)Activator.CreateInstance(typeof(TEventBusType), logger, subscriptionManager, serviceScopeFactory, kafkaConfiguration);
            if (eventBus == null)
            {
                throw new NullReferenceException($"An instance would not be initialized from TEventBusType:{typeof(TEventBusType)}");
            }

            return eventBus;
        });
        
        return serviceCollection;
    }
    
    
    public static IEventBusServiceRegistration AddKafkaEventBus<TEventBusType>(
        this IEventBusServiceRegistration serviceCollection
        , string section
    )
        where TEventBusType : class, IEventBus
    {
        serviceCollection.AddSingleton<TEventBusType>(provider =>
        {
            ILogger<IEventBus> logger = provider.GetRequiredService<ILogger<IEventBus>>();
            IServiceScopeFactory serviceScopeFactory = provider.GetRequiredService<IServiceScopeFactory>();
            IConfiguration configuration = provider.GetRequiredService<IConfiguration>();
            ISubscriptionManager? subscriptionManager = provider.GetService<ISubscriptionManager>();
            if (subscriptionManager == null)
            {
                throw new OptionsValidationException("IEventBusSubscriptionManager", typeof(ISubscriptionManager),
                    new[] { "Please register first the `IEventBusSubscriptionManager` with using `service.AddEventBus()` method!" });
            }

            KafkaServiceConfiguration kafkaConfiguration = new KafkaServiceConfiguration();
            configuration.GetSection(section).Bind(kafkaConfiguration); 
            if (string.IsNullOrEmpty(kafkaConfiguration.Server))
            {
                throw new OptionsValidationException("Server", typeof(string),
                    new[] { "Kafka Server is not defined!" });
            }
            
            if (string.IsNullOrEmpty(kafkaConfiguration.Port))
            {
                throw new OptionsValidationException("Port", typeof(string),
                    new[] { "Kafka Port is not defined!" });
            }
            
            if (string.IsNullOrEmpty(kafkaConfiguration.ConsumerGroupId))
            {
                throw new OptionsValidationException("ConsumerGroupId", typeof(string),
                    new[] { "Kafka ConsumerGroupId is not defined!" });
            }

            TEventBusType? eventBus = (TEventBusType)Activator.CreateInstance(typeof(TEventBusType), logger, subscriptionManager, serviceScopeFactory, kafkaConfiguration);
            if (eventBus == null)
            {
                throw new NullReferenceException($"An instance would not be initialized from TEventBusType:{typeof(TEventBusType)}");
            }

            return eventBus;
        });
        
        return serviceCollection;
    }
}