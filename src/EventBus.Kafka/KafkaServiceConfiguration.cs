namespace FonRadar.Base.EventBus.Kafka;

public record KafkaServiceConfiguration
{
    public string Server { get; set; }
    public string Port { get; set; }
    public string ConsumerGroupId { get; set; }
}