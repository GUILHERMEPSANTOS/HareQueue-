using Microsoft.Extensions.Hosting;

public class ConsumerServer : IHostedService
{
    private readonly IConsumerHandlerRegistry _registry;

    public ConsumerServer(IConsumerHandlerRegistry registry)
    {
        _registry = registry;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var consumers = _registry.ResolveConsumers().ToList();

        if (!consumers?.Any() ?? false) 
            throw new Exception("Nenhum consumer registrado.");

        foreach (var consumer in consumers)
        {
            await consumer.InitializeAsync(cancellationToken);

            _ = Task.Factory.StartNew(
                () => consumer.StartAsync(cancellationToken),
                cancellationToken,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default
            );
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
