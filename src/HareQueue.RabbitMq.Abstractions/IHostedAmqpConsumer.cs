using Microsoft.Extensions.Hosting;

namespace HareQueue.RabbitMq.Abstractions
{
    public interface IHostedAmqpConsumer : IHostedService, IAsyncDisposable;
}
