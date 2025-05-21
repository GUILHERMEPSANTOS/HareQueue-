using HareQueue.ConsoleApp;
using HareQueue.RabbitMq;
using HareQueue.RabbitMq.Context;
using Microsoft.Extensions.Hosting;

var host = Host.CreateDefaultBuilder()
    .ConfigureServices((context, services) =>
    {
        services.AddHareQueue((consumer) =>
        {
            consumer.AddConsumer<UserCreatedEvent>(ExchangeType.Direct);

        }, AssemblyReference.Assembly);

    })
    .Build();

await host.RunAsync();
