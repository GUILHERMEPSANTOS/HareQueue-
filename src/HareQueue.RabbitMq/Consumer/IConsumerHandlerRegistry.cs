using HareQueue.RabbitMq.Consumer;

public interface IConsumerHandlerRegistry
{
    IEnumerable<IQueueConsumer> ResolveConsumers();
}