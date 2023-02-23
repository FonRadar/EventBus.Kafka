using System;
using FonRadar.Base.EventBus.Internal;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
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
        serviceCollection.AddTransient<KafkaServiceConfigurationValidator>();
        serviceCollection.AddSingleton<TEventBusType>(provider =>
        {
            ILogger<IEventBus> logger = provider.GetRequiredService<ILogger<IEventBus>>();
            IServiceScopeFactory serviceScopeFactory = provider.GetRequiredService<IServiceScopeFactory>();
            ISubscriptionManager? subscriptionManager = provider.GetService<ISubscriptionManager>();
            KafkaServiceConfigurationValidator validator = provider.GetRequiredService<KafkaServiceConfigurationValidator>();
            if (subscriptionManager == null)
            {
                throw new OptionsValidationException("IEventBusSubscriptionManager", typeof(ISubscriptionManager),
                    new[] { "Please register first the `IEventBusSubscriptionManager` with using `service.AddEventBus()` method!" });
            }
            
            KafkaServiceConfiguration kafkaConfiguration = new KafkaServiceConfiguration();
            configure(kafkaConfiguration);
            validator.Validate(kafkaConfiguration);

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
        serviceCollection.AddTransient<KafkaServiceConfigurationValidator>();
        serviceCollection.AddSingleton<TEventBusType>(provider =>
        {
            ILogger<IEventBus> logger = provider.GetRequiredService<ILogger<IEventBus>>();
            IServiceScopeFactory serviceScopeFactory = provider.GetRequiredService<IServiceScopeFactory>();
            IConfiguration configuration = provider.GetRequiredService<IConfiguration>();
            ISubscriptionManager? subscriptionManager = provider.GetService<ISubscriptionManager>();
            KafkaServiceConfigurationValidator validator = provider.GetRequiredService<KafkaServiceConfigurationValidator>();
            if (subscriptionManager == null)
            {
                throw new OptionsValidationException("IEventBusSubscriptionManager", typeof(ISubscriptionManager),
                    new[] { "Please register first the `IEventBusSubscriptionManager` with using `service.AddEventBus()` method!" });
            }

            KafkaServiceConfiguration kafkaConfiguration = new KafkaServiceConfiguration();
            configuration.GetSection(section).Bind(kafkaConfiguration);
            validator.Validate(kafkaConfiguration);

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