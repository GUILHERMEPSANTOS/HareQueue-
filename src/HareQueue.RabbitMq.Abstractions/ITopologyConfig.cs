using HareQueue.RabbitMq.Abstractions.Consumer;

namespace HareQueue.RabbitMq.Abstractions
{
    public class ITopologyConfig<TIntegrationEvent> where TIntegrationEvent : IIntegrationEvent
    {
        string Exchange { get; }
        string Queue { get; }
        string RoutingKey { get; }
        string ExchangeType { get; }

        Task BindAsync(IChannelContext channelContext, CancellationToken cancellationToken);
    }
}
