using HareQueue.ConsoleApp;
using HareQueue.RabbitMq.Consumer;

namespace HareQueue.Second.ConsoleApp
{
    public class UserCreatedHandler : IConsumerHandler<UserCreatedEvent>
    {
        public Task Handle(UserCreatedEvent message, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }
    }
}
