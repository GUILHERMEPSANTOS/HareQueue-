using HareQueue.RabbitMq.Abstractions;
using HareQueue.RabbitMq.Abstractions.Consumer;
using HareQueue.RabbitMq.Context;

namespace HareQueue.RabbitMq.Topology
{
    public class DirectTopologyStrategy<TIntegrationEvent> : ITopologyConfigStrategy<TIntegrationEvent>
        where TIntegrationEvent : IIntegrationEvent
    {
        public string Name => typeof(TIntegrationEvent).Name;
        public string Exchange => Name + ".exchange";
        public string Queue => Name + ".queue";
        public string RoutingKey => Name;
        public string ExchangeType => Context.ExchangeType.Direct.ToExchangeName();
        public const bool Durable = true;
        public const bool AutoDelete = false;
        public const bool Exclusive = true;

        public async Task BindAsync(IChannelContext channelContext, CancellationToken cancellationToken)
        {
            await channelContext
                .ExchangeDeclareAsync(exchange: Exchange, type: Context.ExchangeType.Direct, durable: Durable, autoDelete: AutoDelete, cancellationToken: cancellationToken);
            await channelContext
                .QueueDeclareAsync(queue: Queue, durable: Durable, exclusive: Exclusive, autoDelete: AutoDelete, cancellationToken: cancellationToken);
            await channelContext
                .QueueBindAsync(queue: Queue, exchange: Exchange, routingKey: RoutingKey, cancellationToken: cancellationToken);
        }
    }
}
