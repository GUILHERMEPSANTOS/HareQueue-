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
        private readonly Delegate _handler;

        public Dispatcher(Delegate handler)
        {
            _handler = handler;
        }

        public async Task DispatchAsync(IAmqpContext context)
        {
            var amqpArgument = new AmqpArgument<TIntegrationEvent>(context);

            _handler.DynamicInvoke(amqpArgument.GetValue(), context.CancellationToken);

            await context.Channel.BasicAckAsync(
                deliveryTag: context.Request.DeliveryTag,
                multiple: false
            );
        }
    }
}
