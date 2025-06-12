using HareQueue.RabbitMq.Abstractions.Consumer;

namespace HareQueue.ConsoleApp
{
    public class UserCreatedEvent : IIntegrationEvent
    {
        public string Name { get; set; }
    }
}
