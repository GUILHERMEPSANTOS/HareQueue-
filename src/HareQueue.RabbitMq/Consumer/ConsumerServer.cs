using HareQueue.RabbitMq.Context;
using HareQueue.RabbitMq.Serializer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using System.Reflection;

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
            throw new Exception($"Não existe nenhum Handler no Assembly: {_assembly.FullName}");

      
        foreach (var handlerType in consumerHandlerTypes)
        {
            var handlerInterface = handlerType
                    .GetInterfaces()
                    .Where(handler => handler.IsGenericType)
                    .FirstOrDefault();

            var handlerGenericType = handlerInterface.GetGenericArguments().First()!;

            //TODO: verificar se o handler é um IntegrationEvent

            var consumerHandlerType = typeof(IConsumerHandler<>).MakeGenericType(handlerGenericType);

            var consumerHandler = _serviceProvider.GetRequiredService(consumerHandlerType);

            if (consumerHandler is null)
                throw new Exception($"O tipo {consumerHandlerType.FullName} não foi inicializado no DI.");

            var method = handlerInterface.GetMethods().First().Name;
            var handleDelegateType = typeof(Func<,,>).MakeGenericType(handlerGenericType, typeof(CancellationToken), typeof(Task));
            var handleDelegate = Delegate.CreateDelegate(handleDelegateType, consumerHandler, method);

            var serializer = _serviceProvider.GetRequiredService<IAmqpSerializer>();
            var queueConsumerType = typeof(QueueConsumer<>).MakeGenericType(handlerGenericType);

            var connection = _serviceProvider.GetRequiredService<IConnection>();

            IChannelContext channel = new RabbitMqChannelContext(connection);
            IQueueConsumer queueConsumer = (IQueueConsumer)Activator.CreateInstance(queueConsumerType, [handleDelegate, serializer, channel])!;

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
