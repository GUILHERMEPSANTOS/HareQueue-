using Microsoft.Extensions.DependencyInjection;
using System.Text.Json;

namespace HareQueue.RabbitMq.Serializer
{
    public static class AmqpSerializerDependencyInjection
    {
        public static IServiceCollection AddAmqpSerializer(this IServiceCollection services, JsonSerializerOptions options)
        {
            services.AddSingleton<IAmqpSerializer>(sp => new AmqpSerializer(options));
            return services;
        }
    }
}
