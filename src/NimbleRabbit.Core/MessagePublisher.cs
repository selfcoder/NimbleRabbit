using RabbitMQ.Client;

namespace NimbleRabbit
{
    public class MessagePublisher : MessagePublisherBase<byte[]>
    {
        public MessagePublisher(MessageChannelBase channel, string exchange = null, string routingKey = null, bool persistent = false)
            : base(channel, exchange, routingKey, persistent)
        {
        }

        protected override byte[] Pack(byte[] message)
        {
            return message;
        }
    }

    public class MessagePublisher<T> : MessagePublisherBase<T>
    {
        private readonly IMessageSerializer<T> _serializer;

        public MessagePublisher(MessageChannelBase channel, string exchange = null, string routingKey = null, bool persistent = false)
            : base(channel, exchange, routingKey, persistent)
        {
            _serializer = channel.GetSerializer<T>();
        }

        protected IMessageSerializer<T> Serializer => _serializer;

        protected override byte[] Pack(T message)
        {
            return _serializer.Pack(message);
        }

        protected override void Config(IBasicProperties props)
        {
            base.Config(props);
            props.Type = typeof(T).AssemblyQualifiedName;
        }
    }
}