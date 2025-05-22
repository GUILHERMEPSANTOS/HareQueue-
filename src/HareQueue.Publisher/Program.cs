using HareQueue.Publisher;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using System.Text;
using System.Text.Json;


var services = new ServiceCollection();

var connectionFactory = new ConnectionFactory
{
    HostName = "localhost",
    UserName = "guest",
    Password = "guest"
};


using var connection = await connectionFactory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();

var message = new UserCreatedEvent { Name = "HareQueue" };
var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message));

await channel.ExchangeDeclareAsync(exchange: $"{nameof(UserCreatedEvent)}.exchange", durable: true, type: ExchangeType.Direct, autoDelete: false);

await channel.BasicPublishAsync(exchange: $"{nameof(UserCreatedEvent)}.exchange", $"{nameof(UserCreatedEvent)}", body);


Console.WriteLine("Mensagem enviada com sucesso!");
Console.ReadLine();


