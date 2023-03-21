using Microsoft.Extensions.Options;

namespace FonRadar.Base.EventBus.Kafka;

public class KafkaServiceConfigurationValidator
{
    public void Validate(KafkaServiceConfiguration kafkaServiceConfiguration)
    {
        if (kafkaServiceConfiguration.IsUsingAuthentication == null)
        {
            throw new OptionsValidationException("IsUsingAuthentication", typeof(bool),
                new[] { "Kafka IsUsingAuthentication is not defined!" });
        }
        
        if (string.IsNullOrEmpty(kafkaServiceConfiguration.Server))
        {
            throw new OptionsValidationException("Server", typeof(string),
                new[] { "Kafka Server is not defined!" });
        }
            
        if (string.IsNullOrEmpty(kafkaServiceConfiguration.Port))
        {
            throw new OptionsValidationException("Port", typeof(string),
                new[] { "Kafka Port is not defined!" });
        }
            
        if (string.IsNullOrEmpty(kafkaServiceConfiguration.ConsumerGroupId))
        {
            throw new OptionsValidationException("ConsumerGroupId", typeof(string),
                new[] { "Kafka ConsumerGroupId is not defined!" });
        }

        if (kafkaServiceConfiguration.IsUsingAuthentication.Value)
        {
            if (kafkaServiceConfiguration.Username == null)
            {
                throw new OptionsValidationException("Username", typeof(string),
                    new[] { "Kafka Username is not defined!" });
            } 
            
            if (kafkaServiceConfiguration.Password == null)
            {
                throw new OptionsValidationException("Password", typeof(string),
                    new[] { "Kafka Password is not defined!" });
            } 
            
            if (kafkaServiceConfiguration.SaslMechanism == null)
            {
                throw new OptionsValidationException("SaslMechanism", typeof(string),
                    new[] { "Kafka SaslMechanism is not defined!" });
            }
            
            if (kafkaServiceConfiguration.SecurityProtocol == null)
            {
                throw new OptionsValidationException("SecurityProtocol", typeof(string),
                    new[] { "Kafka SecurityProtocol is not defined!" });
            }
        }
    }
}