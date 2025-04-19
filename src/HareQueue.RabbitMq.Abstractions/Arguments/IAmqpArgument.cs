using HareQueue.RabbitMq.Abstractions.Consumer;

namespace HareQueue.RabbitMq.Abstractions.Arguments
{
    public interface IAmqpArgument<TIntegrationEvent> where TIntegrationEvent : IIntegrationEvent
    {
        TIntegrationEvent GetValue(IAmqpContext context);
    }
}
