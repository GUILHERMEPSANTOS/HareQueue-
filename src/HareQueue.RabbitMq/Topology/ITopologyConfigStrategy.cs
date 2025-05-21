using HareQueue.RabbitMq.Abstractions.Consumer;
using HareQueue.RabbitMq.Context;

namespace HareQueue.RabbitMq.Abstractions
{
    public interface ITopologyConfigStrategy<TIntegrationEvent> 
           where TIntegrationEvent : IIntegrationEvent
    {
        string Name { get; }
        string Exchange { get; }
        string Queue { get; }
        string RoutingKey { get; }
        string ExchangeType { get; }

        Task BindAsync(IChannelContext channelContext, CancellationToken cancellationToken);
    }
}
