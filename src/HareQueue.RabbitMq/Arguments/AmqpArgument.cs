using HareQueue.RabbitMq.Abstractions;
using HareQueue.RabbitMq.Abstractions.Arguments;
using HareQueue.RabbitMq.Abstractions.Consumer;

namespace HareQueue.RabbitMq.Arguments
{
    public class AmqpArgument<TIntegrationEvent> : IAmqpArgument<TIntegrationEvent>
        where TIntegrationEvent : IIntegrationEvent
    {
        private readonly IAmqpContext _context;

        public AmqpArgument(IAmqpContext context)
        {
            _context = context;
        }

        public TIntegrationEvent GetValue()
        {
            return (TIntegrationEvent)_context.MessageObject;
        }
    }
}
