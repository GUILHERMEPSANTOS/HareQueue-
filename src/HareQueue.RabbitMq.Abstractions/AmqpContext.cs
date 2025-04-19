using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace HareQueue.RabbitMq.Abstractions
{
    public interface IAmqpContext;

    public class AmqpContext : IAmqpContext
    {
        /// <summary>
        /// Gets the delivery event arguments.
        /// </summary>
        public BasicDeliverEventArgs Request { get; }

        /// <summary>
        /// Gets the channel for Amqp operations.
        /// </summary>
        public IChannel Channel { get; }

        /// <summary>
        /// Gets the connection for Amqp operations.
        /// </summary>
        public IConnection Connection { get; }

        /// <summary>
        /// Gets the name of the queue.
        /// </summary>
        public string QueueName { get; }

        /// <summary>
        /// Gets or sets the message to be sent.
        /// </summary>
        public object MessageObject { get; }
       
        /// <summary>
        /// Gets the service provider.
        /// </summary>
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
