using HareQueue.RabbitMq.Consumer;
using HareQueue.RabbitMq.Context;
using HareQueue.RabbitMq.Serializer;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using System.Reflection;

public class ConsumerHandlerRegistry : IConsumerHandlerRegistry
{
    private readonly Assembly _assembly;
    private readonly IServiceProvider _provider;

    public ConsumerHandlerRegistry(Assembly assembly, IServiceProvider provider)
    {
        _assembly = assembly;
        _provider = provider;
    }

    public IEnumerable<IQueueConsumer> ResolveConsumers()
    {
        var consumerTypes = _assembly.DefinedTypes
            .Where(t => typeof(IConsumerHandler).IsAssignableFrom(t) && !t.IsInterface && !t.IsAbstract);

        foreach (var type in consumerTypes)
        {
            var interfaceType = type.GetInterfaces()
                .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IConsumerHandler<>));

            if (interfaceType == null) continue;

            var messageType = interfaceType.GetGenericArguments().First();
            var consumerHandler = _provider.GetRequiredService(interfaceType);

            var method = interfaceType.GetMethod("Handle");
            var delegateType = typeof(Func<,,>).MakeGenericType(messageType, typeof(CancellationToken), typeof(Task));
            var handlerDelegate = Delegate.CreateDelegate(delegateType, consumerHandler, method);

            var serializer = _provider.GetRequiredService<IAmqpSerializer>();
            var connection = _provider.GetRequiredService<IConnection>();
            var channel = new RabbitMqChannelContext(connection);

            var queueConsumerType = typeof(QueueConsumer<>).MakeGenericType(messageType);
            var queueConsumer = (IQueueConsumer)Activator.CreateInstance(queueConsumerType, handlerDelegate, serializer, channel)!;

            yield return queueConsumer;
        }
    }
}
