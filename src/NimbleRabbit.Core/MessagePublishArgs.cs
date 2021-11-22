using System;
using RabbitMQ.Client;

namespace NimbleRabbit
{
    public class MessagePublishArgs<T>
    {
        public T Message { get; }
        public string Exchange { get; set; }
        public string RoutingKey { get; set; }
        public byte[] Body { get; set; }
        public IBasicProperties Properties { get; }

        public MessagePublishArgs(T message, IBasicProperties properties)
        {
            Message = message;
            if (message is byte[] bytes)
            {
                Body = bytes;
            }

            Properties = properties ?? throw new ArgumentNullException(nameof(properties));
            Exchange = "";
            RoutingKey = "";
        }
    }
}