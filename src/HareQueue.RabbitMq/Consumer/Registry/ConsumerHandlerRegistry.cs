using HareQueue.RabbitMq.Abstractions;
using HareQueue.RabbitMq.Consumer;
using HareQueue.RabbitMq.Context;
using HareQueue.RabbitMq.Serializer;
using HareQueue.RabbitMq.Topology;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using System.Reflection;

public class ConsumerHandlerRegistry : IConsumerHandlerRegistry
{
    private readonly IAssemblyProvider _assemblyProvider;
    private readonly IServiceProvider _serviceProvider;
    private readonly IConsumerTopologyRegistry _topologyRegistry;
    private readonly ITopologyConfigResolver _topologyResolver;

    public ConsumerHandlerRegistry(
        IAssemblyProvider assemblyProvider,
        IServiceProvider serviceProvider,
        IConsumerTopologyRegistry topologyRegistry,
        ITopologyConfigResolver topologyResolver)
    {
        _assemblyProvider = assemblyProvider;
        _serviceProvider = serviceProvider;
        _topologyRegistry = topologyRegistry;
        _topologyResolver = topologyResolver;
    }

    public IEnumerable<IQueueConsumer> ResolveConsumers()
    {
        var consumerTypes = GetConsumerTypes();

        foreach (var consumerType in consumerTypes)
        {
            var interfaceType = GetGenericConsumerInterface(consumerType);
            if (interfaceType == null) continue;

            var messageType = interfaceType.GetGenericArguments().First();
            var handlers = _serviceProvider.GetServices(interfaceType);
            var handleMethod = interfaceType.GetMethod("Handle");

            var handlerDelegates = BuildHandlerDelegates(handlers, handleMethod, messageType);

            var serializer = _serviceProvider.GetRequiredService<IAmqpSerializer>();
            var connection = _serviceProvider.GetRequiredService<IConnection>();
            var channelContext = new RabbitMqChannelContext(connection);

            var topologyInfo = _topologyRegistry.GetConsumerTopology(messageType);
            var topology = ResolveTopology(messageType, topologyInfo);

            var consumer = CreateQueueConsumer(messageType, handlerDelegates, serializer, channelContext, topology);

            yield return consumer;
        }
    }

    private IEnumerable<TypeInfo> GetConsumerTypes() =>
        _assemblyProvider.GetAssembly().DefinedTypes
            .Where(t => typeof(IConsumerHandler).IsAssignableFrom(t) && !t.IsInterface && !t.IsAbstract);

    private static Type? GetGenericConsumerInterface(Type type) =>
        type.GetInterfaces().FirstOrDefault(i =>
            i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IConsumerHandler<>));

    private static List<Delegate> BuildHandlerDelegates(IEnumerable<object> handlers, MethodInfo method, Type messageType)
    {
        var delegateType = typeof(Func<,,>).MakeGenericType(messageType, typeof(CancellationToken), typeof(Task));
        return handlers.Select(handler => Delegate.CreateDelegate(delegateType, handler, method)).ToList();
    }

    private ITopologyConfigStrategy ResolveTopology(Type messageType, ConsumerConfig config)
    {
        var method = typeof(ITopologyConfigResolver)
            .GetMethod(nameof(ITopologyConfigResolver.Resolve))!
            .MakeGenericMethod(messageType);

        return (ITopologyConfigStrategy)method.Invoke(_topologyResolver, [config])!;
    }

    private IQueueConsumer CreateQueueConsumer(
        Type messageType,
        List<Delegate> handlers,
        IAmqpSerializer serializer,
        IChannelContext channelContext,
        ITopologyConfigStrategy topology)
    {
        var consumerType = typeof(QueueConsumer<>).MakeGenericType(messageType);
        return (IQueueConsumer)Activator.CreateInstance(consumerType, handlers, serializer, channelContext, topology)!;
    }
}
