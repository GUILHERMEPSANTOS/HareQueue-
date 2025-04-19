using HareQueue.RabbitMq.Abstractions;

namespace HareQueue.RabbitMq.Consumer.Dispatch
{
    public interface IDispatcher
    {
        Task DispatchAsync(IAmqpContext context);
    }

    public class Dispatcher : IDispatcher
    {
        private readonly Delegate _handler;
        

        public Dispatcher(Delegate handler)
        {
            _handler = handler;
            

        }

        public Task DispatchAsync(IAmqpContext context)
        {
        //    _handler.DynamicInvoke(context.Message, context.CancellationToken);

            return Task.CompletedTask;
        }
    }
}
