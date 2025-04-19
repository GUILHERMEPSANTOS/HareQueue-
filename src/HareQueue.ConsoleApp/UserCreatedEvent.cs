using HareQueue.RabbitMq.Abstractions.Consumer;

namespace HareQueue.ConsoleApp
{
    class UserCreatedEvent : IIntegrationEvent
    {
        public string Name { get; set; }
    }
}
