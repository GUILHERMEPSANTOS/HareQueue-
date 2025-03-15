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
    private IConnection connection;
    private IChannel channel;
    private AsyncEventingBasicConsumer asyncBasicConsumer;

    public bool IsInitialized { get; private set; }

    public QueueConsumer(IConsumerHandler<TMessage> consumerHandler, IServiceProvider serviceProvider)
    {
        _consumerHandler = consumerHandler;
        _serviceProvider = serviceProvider;
    }

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        if (this.IsInitialized) throw new InvalidOperationException("O consumer j[a foi inicializado!");

        connection = _serviceProvider.GetRequiredService<IConnection>();
        this.channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

        asyncBasicConsumer.ReceivedAsync += this.ReceiveAsync;

        IsInitialized = true;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
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
