using System;
using HareQueue.RabbitMq.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace HareQueue.RabbitMq.Consumer;


public interface IQueueConsumer : IHostedAmqpConsumer
{
    Task InitializeAsync(CancellationToken cancellationToken);
};

public class QueueConsumer<TMessage> : IQueueConsumer
{
    public readonly IServiceProvider _serviceProvider;
    private readonly IConsumerHandler<TMessage> _consumerHandler;
    private readonly string _consumerName;
    private IConnection Connection;
    private IChannel Channel;
    private string Queue;
    private string Exchange;
    private string RoutingKey;
    private string ConsumerTag;
    private AsyncEventingBasicConsumer AsyncBasicConsumer;
    private CancellationTokenSource CancellationTokenSource;

    public bool IsInitialized { get; private set; }

    public QueueConsumer(IConsumerHandler<TMessage> consumerHandler, IServiceProvider serviceProvider)
    {
        _consumerHandler = consumerHandler;
        _serviceProvider = serviceProvider;
        _consumerName = typeof(TMessage).Name;
    }

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        if (IsInitialized) throw new InvalidOperationException("O consumer já foi inicializado!");

        Queue = $"{_consumerName}-queue";
        Exchange = $"{_consumerName}-exchange";
        RoutingKey = $"{_consumerName}-routingKey";

        Connection = _serviceProvider.GetRequiredService<IConnection>();

        if (Connection is null)
            throw new Exception("Conexão com RabbitMq n'ao inicializada");

        Channel = await Connection.CreateChannelAsync(cancellationToken: cancellationToken);

        await Channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

        AsyncBasicConsumer = new AsyncEventingBasicConsumer(Channel);
        AsyncBasicConsumer.ReceivedAsync += ReceiveAsync;

        IsInitialized = true;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (!IsInitialized) throw new InvalidOperationException("O consumer não foi inicializado!");

        CancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        await Channel.ExchangeDeclareAsync(exchange: Exchange, durable: true, type: "direct", autoDelete: false);

        await Channel.QueueDeclareAsync(queue: Queue, durable: true, exclusive: false, autoDelete: false);

        await Channel.QueueBindAsync(queue: $"{_consumerName}-queue", exchange: $"{_consumerName}-exchange", routingKey: $"{_consumerName}-routingKey");

        ConsumerTag = await Channel.BasicConsumeAsync(
            queue: Queue,
            autoAck: false,
            consumer: AsyncBasicConsumer,
            arguments: null,
            exclusive: false,
            cancellationToken: CancellationTokenSource.Token,
            consumerTag: ConsumerTag,
            noLocal: true
        );
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }


    private Task ReceiveAsync(object sender, BasicDeliverEventArgs eventArgs)
    {
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }
}
