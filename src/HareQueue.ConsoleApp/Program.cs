using HareQueue.ConsoleApp;
using HareQueue.RabbitMq;
using Microsoft.Extensions.Hosting;

var host = Host.CreateDefaultBuilder()
    .ConfigureServices((context, services) =>
    {     
        services.AddHareQueue(AssemblyReference.Assembly);
    })
    .Build();

await host.RunAsync();
