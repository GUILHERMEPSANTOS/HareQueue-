using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace HareQueue.RabbitMq.Abstractions
{
    public interface IAmqpContext
    {
        public BasicDeliverEventArgs Request { get; }

        public IChannel Channel { get; }

        public IConnection Connection { get; }

        public string QueueName { get; }

        public object MessageObject { get; }

        public CancellationToken CancellationToken { get; }
    }

    public class AmqpContext : IAmqpContext
    {        
        public BasicDeliverEventArgs Request { get; }
        
        public IChannel Channel { get; }
     
        public IConnection Connection { get; }

        public string QueueName { get; }
       
        public object MessageObject { get; }
       
        public CancellationToken CancellationToken { get; }

        public AmqpContext(BasicDeliverEventArgs request, IChannel channel, IConnection connection, string queueName, object messageObject, CancellationToken cancellationToken)
        {
            Request = request;
            Channel = channel;
            Connection = connection;
            QueueName = queueName;
            MessageObject = messageObject;
            CancellationToken = cancellationToken;
        }
    }
}
