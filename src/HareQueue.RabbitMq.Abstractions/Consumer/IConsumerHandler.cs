using HareQueue.RabbitMq.Abstractions.Consumer;

namespace HareQueue.RabbitMq.Consumer
{
    public interface IConsumerHandler<TMessage> : IConsumerHandler where TMessage : IIntegrationEvent
    {
        Task Handle(TMessage message, CancellationToken cancellationToken = default);
    }

    public interface IConsumerHandler;
}
