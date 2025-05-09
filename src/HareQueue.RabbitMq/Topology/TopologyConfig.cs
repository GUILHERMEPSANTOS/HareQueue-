using HareQueue.RabbitMq.Abstractions.Consumer;

namespace HareQueue.RabbitMq.Topology
{
    public class TopologyConfig<TIntegrationEvent>
         where TIntegrationEvent : IIntegrationEvent
    {
        public string Queue { get; set; }
        public string Exchange { get; set; }
        public string RoutingKey { get; set; }

        public TopologyConfig()
        {         
            var consumerName = typeof(TIntegrationEvent).Name;
            
            Queue = $"{consumerName}-queue";
            Exchange = $"{consumerName}-exchange";
            RoutingKey = $"{consumerName}-routingKey";
        }

    }
}
