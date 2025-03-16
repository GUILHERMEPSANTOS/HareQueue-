using HareQueue.RabbitMq.Consumer;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace HareQueue.RabbitMq
{
    public static class HareQueueConfiguration
    {
        public static IServiceCollection AddHareQueue(this IServiceCollection services, Assembly assembly)
        {
            services.AddHostedService(sp => new ConsumerServer(assembly, sp));
            services.AddRabbitMq();

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
    }
}
