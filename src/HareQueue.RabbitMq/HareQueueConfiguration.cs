using HareQueue.RabbitMq.Consumer;
using HareQueue.RabbitMq.Serializer;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using System.Reflection;
using System.Text.Json;


namespace HareQueue.RabbitMq
{
    public static class HareQueueConfiguration
    {
        public static IServiceCollection AddHareQueue(this IServiceCollection services, Assembly assembly)
        {
            services.AddHostedService(sp => new ConsumerServer(assembly, sp));
            services.AddHandlers(assembly);
            services.AddRabbitMq();

            //TODO: deixar configuravel
            services.AddAmqpSerializer(new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true,                
            });

            return services;
        }

        public static IServiceCollection AddRabbitMq(this IServiceCollection services)
        {
            services.AddSingleton<IConnectionFactory>(sp =>
            {
                return new ConnectionFactory
                {
                    HostName = "localhost",
                    UserName = "guest",
                    Password = "guest"
                };
            });

            services.AddSingleton(sp =>
            {
                var connectionFactory = sp.GetRequiredService<IConnectionFactory>();

                return connectionFactory.CreateConnectionAsync().Result;
            });

            return services;
        }

        public static IServiceCollection AddHandlers(this IServiceCollection services, Assembly assembly)
        {
            var consumerHandlerTypes = assembly
                .DefinedTypes
                .Where(IsAssignableToType<IConsumerHandler>);

            foreach (var handlerType in consumerHandlerTypes)
            {
                var handlerInterface = handlerType
                        .GetInterfaces()
                        .Where(handler => handler.IsGenericType)
                        .FirstOrDefault();

                var handlerGenericType = handlerInterface.GetGenericArguments().First()!;
                var consumerHandlerType = typeof(IConsumerHandler<>).MakeGenericType(handlerGenericType);

                services.AddScoped(consumerHandlerType, handlerType);                
            }

            return services;
        }

        private static bool IsAssignableToType<T>(TypeInfo typeInfo)
            => typeof(T).IsAssignableFrom(typeInfo) &&
                !typeInfo.IsAbstract &&
                !typeInfo.IsInterface;
    }
}
