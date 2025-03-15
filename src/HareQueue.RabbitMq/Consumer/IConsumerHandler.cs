namespace HareQueue.RabbitMq.Consumer
{
    public interface IConsumerHandler<TMessage> : IConsumerHandler
    {
        Task Handle(TMessage message);
    }

    public interface IConsumerHandler;
}
