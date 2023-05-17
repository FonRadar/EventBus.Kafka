using System;

namespace FonRadar.Base.EventBus.Kafka;

/// <summary>
/// Contains the configuration values for Kafka connection
/// </summary>
public record KafkaServiceConfiguration
{
    /// <summary>
    /// Definition of the kafka Server Address or Ip
    /// </summary>
    public string Server { get; set; }
    
    /// <summary>
    /// Definition of the kafka port value
    /// </summary>
    public string Port { get; set; }
    
    /// <summary>
    /// Definition of the kafka ConsumerGroupId value
    /// </summary>
    public string ConsumerGroupId { get; set; }
    
    /// <summary>
    /// If authentication is to be used, it must be set to true.
    /// </summary>
    public bool? IsUsingAuthentication { get; set; }
    
    /// <summary>
    /// Kafka Username value for authentication
    /// </summary>
    public string Username { get; set; }
    
    /// <summary>
    /// Kafka Password value for authentication
    /// </summary>
    public string Password { get; set; }
    
    /// <summary>
    /// Kafka <seealso cref="Confluent.Kafka.SaslMechanism">SaslMechanism</seealso> value for authentication
    /// </summary>
    public string SaslMechanism { get; set; }
    
    /// <summary>
    /// Kafka <seealso cref="Confluent.Kafka.SecurityProtocol">SecurityProtocol</seealso> value for authentication
    /// </summary>
    public string SecurityProtocol { get; set; }

    /// <summary>
    /// Retry Count value for producer.
    /// </summary>
    public int RetryCount { get; set; } = 3;

    /// <summary>
    /// Dead Letter value for Kafka Event Bus. If enabled all failed consumer events will be published to Dead Letter topic.
    /// </summary>
    public bool EnableDeadLetter { get; set; } = false;
}