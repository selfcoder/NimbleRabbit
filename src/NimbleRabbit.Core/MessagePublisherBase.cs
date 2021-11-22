using System;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace NimbleRabbit
{
    public abstract class MessagePublisherBase<T> : IMessagePublisher<T>
    {
        private readonly MessageChannelBase _channel;
        private readonly string _exchange;
        private readonly string _routingKey;
        private readonly bool _persistent;
        private readonly Action<IBasicProperties> _propsConfig;

        public MessagePublisherBase(MessageChannelBase channel, string exchange = null, string routingKey = null, bool persistent = false)
        {
            _channel = channel;
            _exchange = exchange;
            _routingKey = routingKey;
            _persistent = persistent;
            _propsConfig = Config;
        }

        public string Exchange => _exchange;

        public string RoutingKey => _routingKey;

        public virtual Task Publish(T message, Action<IBasicProperties> propertiesConfig = null)
        {
            return Publish(_exchange, _routingKey, message, propertiesConfig);
        }

        protected Task Publish(string exchange, string routingKey, T message, Action<IBasicProperties> propertiesConfig = null)
        {
            if (exchange == null) throw new ArgumentNullException(nameof(exchange));

            byte[] data = Pack(message);
            Action<IBasicProperties> propsConfig;
            if (propertiesConfig == null)
            {
                propsConfig = _propsConfig;
            }
            else
            {
                propsConfig = p =>
                {
                    _propsConfig(p);
                    propertiesConfig(p);
                };
            }

            return _channel.PublishAsync(exchange, routingKey, data, propsConfig);
        }

        protected MessageChannelBase Channel => _channel;

        protected abstract byte[] Pack(T message);

        protected virtual void Config(IBasicProperties props)
        {
            props.Persistent = _persistent;
        }

        Task IMessagePublisher<T>.Publish(T message)
        {
            return Publish(message);
        }
    }
}