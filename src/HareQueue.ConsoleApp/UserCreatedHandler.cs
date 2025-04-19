using HareQueue.RabbitMq.Consumer;

namespace HareQueue.ConsoleApp
{
    class UserCreatedHandler : IConsumerHandler<UserCreatedEvent>
    {
        public Task Handle(UserCreatedEvent message, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }
    }
}
