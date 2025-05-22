using HareQueue.RabbitMq.Abstractions;
using System.Reflection;

namespace HareQueue.ConsoleApp
{
    public class DefaultAssemblyProvider : IAssemblyProvider
    {
        private readonly Assembly _assembly;

        public DefaultAssemblyProvider(Assembly assembly)
        {
            _assembly = assembly;
        }

        public Assembly GetAssembly() => _assembly;
    }
}
