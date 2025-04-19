using HareQueue.RabbitMq.Abstractions;
using HareQueue.RabbitMq.Abstractions.Consumer;
using HareQueue.RabbitMq.Consumer.Dispatch;
using HareQueue.RabbitMq.Serializer;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace HareQueue.RabbitMq.Consumer;


public interface IQueueConsumer : IHostedAmqpConsumer
{
    Task InitializeAsync(CancellationToken cancellationToken);
};

public class QueueConsumer<TMessage> : IQueueConsumer
    where TMessage : IIntegrationEvent
{
    public readonly IServiceProvider ServiceProvider;
    private readonly Delegate _handler;
    private readonly string _consumerName;
    private IConnection _connection;
    private IChannel _channel;
    private string _queue;
    private string _exchange;
    private string _routingKey;
    private string _consumerTag;
    private Dispatcher _dispatcher;
    private AsyncEventingBasicConsumer _asyncBasicConsumer;
    private CancellationTokenSource _cancellationTokenSource;
    private IAmqpSerializer _serializer;

    public bool IsInitialized { get; private set; }

    public QueueConsumer(Delegate handler, IAmqpSerializer serializer, IServiceProvider serviceProvider)
    {
        _handler = handler;
        _serializer = serializer;
        ServiceProvider = serviceProvider;
        _consumerName = typeof(TMessage).Name;
    }

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        if (IsInitialized) throw new InvalidOperationException("O consumer já foi inicializado!");

        _queue = $"{_consumerName}-queue";
        _exchange = $"{_consumerName}-exchange";
        _routingKey = $"{_consumerName}-routingKey";

        _dispatcher = new Dispatcher(_handler);

        _connection = ServiceProvider.GetRequiredService<IConnection>();

        if (_connection is null)
            throw new Exception("Conexão com RabbitMq n'ao inicializada");

        _channel = await _connection.CreateChannelAsync(cancellationToken: cancellationToken);

        await _channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false, cancellationToken: cancellationToken);

        _asyncBasicConsumer = new AsyncEventingBasicConsumer(_channel);
        _asyncBasicConsumer.ReceivedAsync += ReceiveAsync;

        IsInitialized = true;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (!IsInitialized) throw new InvalidOperationException("O consumer não foi inicializado!");

        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        await _channel.ExchangeDeclareAsync(exchange: _exchange, durable: true, type: "direct", autoDelete: false);

        await _channel.QueueDeclareAsync(queue: _queue, durable: true, exclusive: false, autoDelete: false);

        await _channel.QueueBindAsync(queue: _queue, exchange: _exchange, routingKey: _routingKey);

        _consumerTag = await _channel.BasicConsumeAsync(
            queue: _queue,
            autoAck: false,
            consumer: _asyncBasicConsumer,
            arguments: null,
            exclusive: false,
            cancellationToken: _cancellationTokenSource.Token,
            consumerTag: _consumerTag,
            noLocal: true
        );
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }


    private Task ReceiveAsync(object sender, BasicDeliverEventArgs eventArgs)
    {
        var message = _serializer.Deserialize<TMessage>(eventArgs);

        IAmqpContext context = new AmqpContext(eventArgs, _channel, _connection, _queue, message, _cancellationTokenSource.Token);

        _dispatcher.DispatchAsync(context);
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }
}
