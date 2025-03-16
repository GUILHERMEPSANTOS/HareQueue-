using HareQueue.ConsoleApp;
using HareQueue.RabbitMq;
using HareQueue.RabbitMq.Consumer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var host = Host.CreateDefaultBuilder()
    .ConfigureServices((context, services) =>
    {
        services.AddScoped<IConsumerHandler<UserCreatedEvent>, UserCreatedHandler>();
        services.AddHareQueue(typeof(Program).Assembly);
    })
    .Build();

await host.RunAsync();
