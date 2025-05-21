using HareQueue.RabbitMq.Abstractions;
using HareQueue.RabbitMq.Abstractions.Consumer;
using HareQueue.RabbitMq.Consumer.Dispatch;
using HareQueue.RabbitMq.Context;
using HareQueue.RabbitMq.Serializer;
using HareQueue.RabbitMq.Topology;
using RabbitMQ.Client.Events;

namespace HareQueue.RabbitMq.Consumer;


public interface IQueueConsumer : IHostedAmqpConsumer
{
    Task InitializeAsync(CancellationToken cancellationToken);
};

public class QueueConsumer<TIntegrationEvent> : IQueueConsumer
    where TIntegrationEvent : IIntegrationEvent
{
    private readonly Delegate _handler;    
    private readonly IChannelContext _channelContext;
    private Dispatcher<TIntegrationEvent> _dispatcher;
    private CancellationTokenSource _cancellationTokenSource;
    private IAmqpSerializer _serializer;
    private bool _isConsuming { get; set; }
    private bool _isInitialized { get; set; }

    public QueueConsumer(
        Delegate handler,
        IAmqpSerializer serializer,
        IChannelContext channelContext)
    {
        _handler = handler;
        _serializer = serializer;        
        _channelContext = channelContext;
    }

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        if (_isInitialized) throw new InvalidOperationException("O consumer já foi inicializado!");

        _dispatcher = new Dispatcher<TIntegrationEvent>(_handler);

        if (_channelContext is null)
            throw new Exception("channel com RabbitMq não inicializada");

        await _channelContext.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false, cancellationToken: cancellationToken);

        _channelContext.ReceivedAsync += ReceiveAsync;
        _isInitialized = true;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_isInitialized) throw new InvalidOperationException("O consumer não foi inicializado!");

        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        await _channelContext.ExchangeDeclareAsync(exchange: _topologyConfig.Exchange, durable: true, type: ExchangeType.Direct, autoDelete: false);
        await _channelContext.QueueDeclareAsync(queue: _topologyConfig.Queue, durable: true, exclusive: false, autoDelete: false);
        await _channelContext.QueueBindAsync(queue: _topologyConfig.Queue, exchange: _topologyConfig.Exchange, routingKey: _topologyConfig.RoutingKey);
        await _channelContext.BasicConsumeAsync(
           queue: _topologyConfig.Queue,
           autoAck: false,
           arguments: null,
           exclusive: false,
           cancellationToken: _cancellationTokenSource.Token,
           noLocal: true
       );

        _isConsuming = true;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (!_isConsuming) return;

        await _channelContext.BasicCancelAsync(cancellationToken: cancellationToken); ;
    }


    private async Task ReceiveAsync(object sender, BasicDeliverEventArgs eventArgs)
    {
        var message = _serializer.Deserialize<TIntegrationEvent>(eventArgs);

        IAmqpContext context = new AmqpContext(eventArgs, _channelContext.Channel, _channelContext.Connection, _topologyConfig.Queue, message, _cancellationTokenSource.Token);

        await _dispatcher.DispatchAsync(context);
    }

    public ValueTask DisposeAsync()
    {
        _channelContext.Dispose();

        return ValueTask.CompletedTask;
    }
}
