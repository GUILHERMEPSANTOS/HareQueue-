using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace HareQueue.RabbitMq.Consumer;

public class ConsumerServer : IHostedService
{
    public readonly Assembly _assembly;
    public readonly IServiceProvider _serviceProvider;

    public ConsumerServer(Assembly assembly, IServiceProvider serviceProvider)
    {
        _assembly = assembly;
        _serviceProvider = serviceProvider;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var consumerHandlerTypes = _assembly
                .DefinedTypes
                .Where(IsAssignableToType<IConsumerHandler>);

        if (consumerHandlerTypes.Count() == 0)
             throw new Exception($"N�o existe nenhum Handler no Assembly: {_assembly.FullName}");

        foreach (var handlerType in consumerHandlerTypes)
        {
            var handlerInterface = handlerType
                    .GetInterfaces()
                    .Where(handler => handler.IsGenericType)
                    .FirstOrDefault();
            
            var handlerGenericType = handlerInterface.GetGenericArguments().First()!;         
            var consumerHandlerType = typeof(IConsumerHandler<>).MakeGenericType(handlerGenericType);
            
            var consumerHandler = _serviceProvider.GetRequiredService(consumerHandlerType);

            if (consumerHandler is null)
                throw new Exception($"O tipo {consumerHandlerType.FullName} não foi inicializado no DI.");

            var queueConsumerType = typeof(QueueConsumer<>).MakeGenericType(handlerGenericType);                  
            IQueueConsumer queueConsumer = (IQueueConsumer)Activator.CreateInstance(queueConsumerType, [consumerHandler, _serviceProvider]);

            await queueConsumer.InitializeAsync(cancellationToken);

            _ = Task.Factory
                    .StartNew(
                        () => queueConsumer.StartAsync(cancellationToken),
                        cancellationToken,
                        TaskCreationOptions.LongRunning,
                        TaskScheduler.Default
                     );
        }
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    private static bool IsAssignableToType<T>(TypeInfo typeInfo)
        => typeof(T).IsAssignableFrom(typeInfo) &&
            !typeInfo.IsAbstract &&
            !typeInfo.IsInterface;
}
