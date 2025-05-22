using HareQueue.RabbitMq.Abstractions.Consumer;
using HareQueue.RabbitMq.Context;

namespace HareQueue.RabbitMq.Consumer
{
    public interface IConsumerTopologyRegistry
    {
        void AddConsumer<TIntegrationEvent>(ExchangeType type)
            where TIntegrationEvent : IIntegrationEvent;
        ConsumerConfig GetConsumerTopology(Type integrationEvent);
    }


    public class ConsumerTopologyRegitry : IConsumerTopologyRegistry
    {
        private IDictionary<Type, ConsumerConfig> _consumersRegitry = new Dictionary<Type, ConsumerConfig>();

        public void AddConsumer<TIntegrationEvent>(ExchangeType type)
            where TIntegrationEvent : IIntegrationEvent
        {
            _consumersRegitry.Add(typeof(TIntegrationEvent), new ConsumerConfig(type));
        }

        public ConsumerConfig GetConsumerTopology(Type integrationEvent)
        {
            if (_consumersRegitry.TryGetValue(integrationEvent, out var consumerConfig))
            {
                return consumerConfig;
            }

            throw new InvalidOperationException($"Consumer configuration for {integrationEvent} not found.");
        }
    }

    public class ConsumerConfig
    {
        public ExchangeType ExchangeType { get; set; }

        public ConsumerConfig(ExchangeType exchangeType)
        {
            ExchangeType = exchangeType;
        }
    }
}
