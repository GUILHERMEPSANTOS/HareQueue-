using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace HareQueue.RabbitMq.Context
{   
    public class RabbitMqChannelContext : IChannelContext
    {
        private readonly IChannel _channel;
        private readonly IConnection _connection;
        public IChannel Channel => _channel;
        public IConnection Connection => _connection;

        private readonly AsyncEventingBasicConsumer _asyncBasicConsumer;
        public event AsyncEventHandler<BasicDeliverEventArgs> ReceivedAsync;
       
        private string ConsumerTag { get; set; }

        public RabbitMqChannelContext(IConnection connection)
        {
            _connection = connection;
            _channel = connection.CreateChannelAsync().Result;
            _asyncBasicConsumer = new AsyncEventingBasicConsumer(_channel);
        }

        public async Task BasicConsumeAsync(
            string queue,
            bool autoAck,
            bool noLocal,
            bool exclusive,
            IDictionary<string, object?>? arguments,
            CancellationToken cancellationToken = default)
        {
            if (ReceivedAsync is null)
            {
                throw new Exception("O evento ReceivedAsync não foi inicializado.");
            }

            _asyncBasicConsumer.ReceivedAsync += ReceivedAsync;

            ConsumerTag = await _channel.BasicConsumeAsync(
                queue: queue,
                autoAck: autoAck,
                consumer: _asyncBasicConsumer,
                arguments: arguments,
                exclusive: exclusive,
                cancellationToken: cancellationToken,
                consumerTag: string.Empty,
                noLocal: noLocal
             );
        }

        public async Task ExchangeDeclareAsync(string exchange, ExchangeType type, bool durable, bool autoDelete, CancellationToken cancellationToken = default)
        {
            await _channel.ExchangeDeclareAsync(exchange: exchange, durable: true, type: type.ToExchangeName(), autoDelete: false, cancellationToken: cancellationToken);
        }

        public async Task QueueBindAsync(string queue, string exchange, string routingKey, CancellationToken cancellationToken = default)
        {
            await _channel.QueueBindAsync(queue: queue, exchange: exchange, routingKey: routingKey, cancellationToken: cancellationToken);
        }

        public async Task QueueDeclareAsync(string queue, bool durable, bool exclusive, bool autoDelete, CancellationToken cancellationToken = default)
        {
            await _channel.QueueDeclareAsync(queue: queue, durable: durable, exclusive: exclusive, autoDelete: autoDelete, cancellationToken: cancellationToken);
        }

        public async Task BasicCancelAsync(bool noWait = false, CancellationToken cancellationToken = default)
        {
            await _channel.BasicCancelAsync(consumerTag: ConsumerTag, noWait: noWait, cancellationToken: cancellationToken);
        }

        public async Task BasicQosAsync(uint prefetchSize, ushort prefetchCount, bool global, CancellationToken cancellationToken)
        {
            await _channel.BasicQosAsync(prefetchSize: prefetchSize, prefetchCount: prefetchCount, global: global, cancellationToken: cancellationToken);
        }

        public void Dispose()
        {
            if(_channel is { }) _channel.Dispose();                        
        }
    }
}
