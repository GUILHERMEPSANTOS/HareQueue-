using HareQueue.RabbitMq.Abstractions.Consumer;
using HareQueue.RabbitMq.Abstractions;
using HareQueue.RabbitMq.Consumer;


// continuar amanhã
namespace HareQueue.RabbitMq.Topology
{
    public interface ITopologyConfigResolver
    {
        ITopologyConfig<T> Resolve<T>(ConsumerConfig consumerConfig) where T : IIntegrationEvent;
    }

    public class TopologyConfigResolver : ITopologyConfigResolver
    {
        public ITopologyConfig<T> Resolve<T>(ConsumerConfig consumerConfig) where T : IIntegrationEvent
        {
            
        }
    }

    public static class TopologyConfigFactory
    {

    }
}
