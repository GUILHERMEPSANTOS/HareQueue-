using HareQueue.RabbitMq.Abstractions;
using HareQueue.RabbitMq.Abstractions.Consumer;
using HareQueue.RabbitMq.Context;

namespace HareQueue.RabbitMq.Topology
{
    public class FanoutTopologyStrategy<TIntegrationEvent> : ITopologyConfigStrategy<TIntegrationEvent>
        where TIntegrationEvent : IIntegrationEvent
    {
        public string Name => typeof(TIntegrationEvent).Name;
        public string Exchange => Name + ".exchange";
        public string Queue => Name + ".queue";
        public string RoutingKey => string.Empty;
        public string ExchangeType => Context.ExchangeType.Fanout.ToExchangeName();


        public Task BindAsync(IChannelContext channelContext, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
