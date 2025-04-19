using HareQueue.RabbitMq.Abstractions;
using HareQueue.RabbitMq.Abstractions.Arguments;
using HareQueue.RabbitMq.Abstractions.Consumer;

namespace HareQueue.RabbitMq.Arguments
{
    public class AmqpArgument<TIntegrationEvent> : IAmqpArgument<TIntegrationEvent>
        where TIntegrationEvent : IIntegrationEvent
    {
        public TIntegrationEvent GetValue(IAmqpContext context)
        {
            throw new NotImplementedException();
        }
    }
}
