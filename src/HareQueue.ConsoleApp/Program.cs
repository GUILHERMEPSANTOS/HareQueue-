using HareQueue.ConsoleApp;
using HareQueue.RabbitMq.Consumer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var host = Host.CreateDefaultBuilder()
    .ConfigureServices((context, services) =>
    {
        services.AddScoped<IConsumerHandler<UserCreatedEvent>, UserCreatedHandler>();

        services.AddHostedService(sp => new ConsumerServer(typeof(Program).Assembly, sp));
    })
    .Build();

await host.RunAsync();
