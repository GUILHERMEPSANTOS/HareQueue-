using HareQueue.RabbitMq.Abstractions.Consumer;
using HareQueue.RabbitMq.Context;

namespace HareQueue.RabbitMq.Consumer
{
    public interface IConsumerTopologyRegistry
    {
        void AddConsumer<TIntegrationEvent>(ExchangeType type, string? queueName = null)
            where TIntegrationEvent : IIntegrationEvent;
        ConsumerConfig GetConsumerTopology(Type integrationEvent);
    }


    public class ConsumerTopologyRegitry : IConsumerTopologyRegistry
    {
        private IDictionary<Type, ConsumerConfig> _consumersRegitry = new Dictionary<Type, ConsumerConfig>();

        public void AddConsumer<TIntegrationEvent>(ExchangeType type, string? queueName = null)
            where TIntegrationEvent : IIntegrationEvent
        {
            if (type == ExchangeType.Fanout && string.IsNullOrWhiteSpace(queueName))
            {
                throw new ArgumentException("when you using a fanout exchange type, specifying the queue name is mandatory");
            }

            _consumersRegitry.Add(typeof(TIntegrationEvent), new ConsumerConfig(type, queueName ?? string.Empty));
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
        public string? QueueName { get; set; }

        public ConsumerConfig(ExchangeType exchangeType)
        {
            if (exchangeType == ExchangeType.Fanout)
            {
                throw new ArgumentException("when you using a fanout exchange type, specifying the queue name is mandatory");
            }

            ExchangeType = exchangeType;

        }

        public ConsumerConfig(ExchangeType exchangeType, string? queueName)
        {
            if (exchangeType == ExchangeType.Fanout && string.IsNullOrWhiteSpace(queueName))
            {
                throw new ArgumentException("when you using a fanout exchange type, specifying the queue name is mandatory");
            }

            ExchangeType = exchangeType;
            QueueName = queueName;
        }
    }
}
