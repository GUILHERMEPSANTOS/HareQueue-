using HareQueue.RabbitMq.Abstractions;
using HareQueue.RabbitMq.Consumer;
using HareQueue.RabbitMq.Context;
using HareQueue.RabbitMq.Serializer;
using HareQueue.RabbitMq.Topology;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;

public class ConsumerHandlerRegistry : IConsumerHandlerRegistry
{
    private readonly IAssemblyProvider _assembly;
    private readonly IServiceProvider _provider;
    private readonly IConsumerTopologyRegistry _consumerTopologyRegitry;
    private readonly ITopologyConfigResolver _topologyConfigResolver;    

    public ConsumerHandlerRegistry(IAssemblyProvider assembly,
        IServiceProvider provider,
        IConsumerTopologyRegistry consumerTopologyRegitry,
        ITopologyConfigResolver topologyConfigResolver)
    {
        _assembly = assembly;
        _provider = provider;
        _consumerTopologyRegitry = consumerTopologyRegitry;
        _topologyConfigResolver = topologyConfigResolver;
    }

    public IEnumerable<IQueueConsumer> ResolveConsumers()
    {
        var consumerTypes = _assembly.GetAssembly().DefinedTypes
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

            var consumerConfig = _consumerTopologyRegitry.GetConsumerTopology(messageType);

            var getTopologyMethod = typeof(ITopologyConfigResolver)
                .GetMethod(nameof(ITopologyConfigResolver.Resolve))
                .MakeGenericMethod(messageType);

            var topology = getTopologyMethod.Invoke(_topologyConfigResolver, [consumerConfig]) as ITopologyConfigStrategy;

            var queueConsumerType = typeof(QueueConsumer<>).MakeGenericType(messageType);
            var queueConsumer = (IQueueConsumer)Activator.CreateInstance(queueConsumerType, handlerDelegate, serializer, channel, topology)!;

            yield return queueConsumer;
        }
    }
}
