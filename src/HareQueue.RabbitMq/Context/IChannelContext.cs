using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace HareQueue.RabbitMq.Context
{
    public interface IChannelContext : IDisposable
    {
        IChannel Channel { get; }
        IConnection Connection { get; }
        event AsyncEventHandler<BasicDeliverEventArgs> ReceivedAsync;

        Task ExchangeDeclareAsync(string exchange, ExchangeType type, bool durable, bool autoDelete, CancellationToken cancellationToken = default);

        Task QueueDeclareAsync(string queue, bool durable, bool exclusive, bool autoDelete, CancellationToken cancellationToken = default);

        Task QueueBindAsync(string queue, string exchange, string routingKey, CancellationToken cancellationToken = default);

        Task BasicConsumeAsync(string queue, bool autoAck, bool noLocal, bool exclusive,
            IDictionary<string, object?>? arguments, CancellationToken cancellationToken = default);

        Task BasicCancelAsync(bool noWait = false, CancellationToken cancellationToken = default);

        Task BasicQosAsync(uint prefetchSize, ushort prefetchCount, bool global, CancellationToken cancellationToken);
    }
}
