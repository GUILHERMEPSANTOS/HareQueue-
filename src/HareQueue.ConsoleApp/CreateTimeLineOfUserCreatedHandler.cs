using HareQueue.RabbitMq.Consumer;

namespace HareQueue.ConsoleApp
{
    public class CreateTimeLineOfUserCreatedHandler : IConsumerHandler<UserCreatedEvent>
    {
        public Task Handle(UserCreatedEvent message, CancellationToken cancellationToken = default)
        {            
            return Task.CompletedTask;
        }
    }    
}
