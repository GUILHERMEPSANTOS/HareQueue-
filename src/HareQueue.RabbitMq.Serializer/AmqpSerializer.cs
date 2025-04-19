using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;

namespace HareQueue.RabbitMq.Serializer
{
    public interface IAmqpSerializer
    {
        TMessage Deserialize<TMessage>(BasicDeliverEventArgs args);    
    }

    public class AmqpSerializer(JsonSerializerOptions options) : IAmqpSerializer
    {
        private readonly JsonSerializerOptions options = options ?? new();

        public TMessage Deserialize<TMessage>(BasicDeliverEventArgs args)
        {
            if (args is null) throw new ArgumentNullException(nameof(args));

            var bytes = args.Body.ToArray();

            if (bytes.Length == 0) return default;

            var message = Encoding.UTF8.GetString(bytes);

            if (!string.IsNullOrWhiteSpace(message))
            {
                return JsonSerializer.Deserialize<TMessage>(message, options);
            }

            return default;
        }
    }
}
