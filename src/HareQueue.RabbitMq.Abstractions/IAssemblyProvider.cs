using System.Reflection;

namespace HareQueue.RabbitMq.Abstractions
{
    public interface IAssemblyProvider
    {
        Assembly GetAssembly();
    }
}
