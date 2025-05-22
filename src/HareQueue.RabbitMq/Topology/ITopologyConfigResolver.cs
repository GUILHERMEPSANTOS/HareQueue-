using HareQueue.RabbitMq.Abstractions;
using HareQueue.RabbitMq.Abstractions.Consumer;
using HareQueue.RabbitMq.Consumer;

namespace HareQueue.RabbitMq.Topology
{
    public interface ITopologyConfigResolver
    {
        ITopologyConfigStrategy<T> Resolve<T>(ConsumerConfig consumerConfig) where T : IIntegrationEvent;
    }

    public class TopologyConfigResolver : ITopologyConfigResolver
    {
        public ITopologyConfigStrategy<TIntegrationEvent> Resolve<TIntegrationEvent>(ConsumerConfig consumerConfig) where TIntegrationEvent : IIntegrationEvent
        {
            return TopologyConfigFactory.Create<TIntegrationEvent>(consumerConfig);
        }
    }

    public static class TopologyConfigFactory
    {
        public static ITopologyConfigStrategy<TIntegrationEvent> Create<TIntegrationEvent>(ConsumerConfig consumerConfig)
            where TIntegrationEvent : IIntegrationEvent
        {
            return consumerConfig.ExchangeType switch
            {
                Context.ExchangeType.Direct => new DirectTopologyStrategy<TIntegrationEvent>(),
                Context.ExchangeType.Fanout => new FanoutTopologyStrategy<TIntegrationEvent>()
            };
        }
    }
}
