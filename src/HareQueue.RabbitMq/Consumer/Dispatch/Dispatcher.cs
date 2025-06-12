using HareQueue.RabbitMq.Abstractions;
using HareQueue.RabbitMq.Abstractions.Consumer;
using HareQueue.RabbitMq.Arguments;

namespace HareQueue.RabbitMq.Consumer.Dispatch
{
    public interface IDispatcher
    {
        Task DispatchAsync(IAmqpContext context);
    }

    public class Dispatcher<TIntegrationEvent> : IDispatcher
        where TIntegrationEvent : IIntegrationEvent
    {
        private readonly List<Delegate> _handlers;

        public Dispatcher(List<Delegate> handlers)
        {
            _handlers = handlers;
        }

        public async Task DispatchAsync(IAmqpContext context)
        {
            var amqpArgument = new AmqpArgument<TIntegrationEvent>(context);

            foreach (var handler in _handlers)
            {
                handler.DynamicInvoke(amqpArgument.GetValue(), context.CancellationToken);
            }

            await context.Channel.BasicAckAsync(
                deliveryTag: context.Request.DeliveryTag,
                multiple: false
            );
        }
    }
}
