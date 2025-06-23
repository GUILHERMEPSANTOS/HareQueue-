using HareQueue.ConsoleApp;
using HareQueue.RabbitMq;
using HareQueue.RabbitMq.Abstractions;
using HareQueue.RabbitMq.Context;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var host = Host.CreateDefaultBuilder()
    .ConfigureServices((context, services) =>
    {
        //TODO: Melhorar logica do assembly
        services.AddSingleton(AssemblyReference.Assembly);
        services.AddSingleton<IAssemblyProvider, DefaultAssemblyProvider>();
        services.AddHareQueue((consumer) =>
        {
            consumer.AddConsumer<UserCreatedEvent>(ExchangeType.Fanout, "userCreated");

        }, AssemblyReference.Assembly);

    })
    .Build();

await host.RunAsync();
