using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using FonRadar.Base.EventBus.Internal;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace FonRadar.Base.EventBus.Kafka;

public static class ServiceRegistrationExtension
{
    public static IEventBusServiceRegistration AddKafkaEventBus(
        this IEventBusServiceRegistration serviceCollection
        , string section
    )
    {
        serviceCollection.AddKafkaEventBus<IKafkaEventBus, KafkaEventBus>(section);
        return serviceCollection;
    }
    
    public static IEventBusServiceRegistration AddKafkaEventBus(
        this IEventBusServiceRegistration serviceCollection
        , Action<IServiceProvider, KafkaServiceConfiguration> configure
    )
    {
        serviceCollection.AddKafkaEventBus<IKafkaEventBus, KafkaEventBus>(configure);
        return serviceCollection;
    }
    
    public static IEventBusServiceRegistration AddKafkaEventBus<TEventBusType, TEventBusImplementationType>(
        this IEventBusServiceRegistration serviceCollection
        , string section
    )
        where TEventBusType : class, IKafkaEventBus
        where TEventBusImplementationType : class, TEventBusType
    {
        serviceCollection.AddTransient<KafkaServiceConfigurationValidator>();
        serviceCollection.AddKafkaEventBus<TEventBusType, TEventBusImplementationType>((provider, kafkaConfiguration) =>
        {
            IConfiguration configuration = provider.GetRequiredService<IConfiguration>();
            configuration.GetSection(section).Bind(kafkaConfiguration);
        });
    
        return serviceCollection;
    }
    
    public static IEventBusServiceRegistration AddKafkaEventBus<TEventBusType, TEventBusImplementationType>(
        this IEventBusServiceRegistration serviceCollection
        , Action<IServiceProvider, KafkaServiceConfiguration> configure
    )
        where TEventBusType : class, IKafkaEventBus
        where TEventBusImplementationType : class, TEventBusType
    {
        serviceCollection.AddTransient<KafkaServiceConfigurationValidator>();
        
        List<Type> eventHandlers = AppDomain.CurrentDomain.GetAssemblies()
            .SelectMany(x => x.GetTypes())
            .Where(t => t.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEventHandler<>)))
            .ToList();

        foreach (Type eventHandler in eventHandlers)
        {
            serviceCollection.AddScoped(eventHandler);
        }
        
        serviceCollection.AddSingleton<TEventBusType, TEventBusImplementationType>(provider =>
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
            configure(provider, kafkaConfiguration);
            validator.Validate(kafkaConfiguration);

            TEventBusImplementationType? eventBus = (TEventBusImplementationType)Activator.CreateInstance(typeof(TEventBusImplementationType), logger, subscriptionManager, serviceScopeFactory, kafkaConfiguration);
            if (eventBus == null)
            {
                throw new NullReferenceException($"An instance would not be initialized from TEventBusType:{typeof(TEventBusImplementationType)}");
            }

            return eventBus;
        });
        
        return serviceCollection;
    }
}