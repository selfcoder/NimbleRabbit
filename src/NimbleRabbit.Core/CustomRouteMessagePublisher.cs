using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace NimbleRabbit
{
    public class CustomRouteMessagePublisher : MessagePublisher
    {
        public CustomRouteMessagePublisher(MessageChannelBase channel, string exchange = "", bool persistent = false)
            : base(channel, exchange, null, persistent)
        {
        }

        public override Task Publish(byte[] message, Action<IBasicProperties> propertiesConfig = null)
        {
            throw new NotSupportedException();
        }

        public async Task<bool> Publish(string routingKey, byte[] message)
        {
            await base.Publish(Exchange, routingKey, message);
            return true;
        }
    }

    public class CustomRouteMessagePublisher<T> : MessagePublisher<T>
    {
        private bool _declareQueues;
        private ConcurrentDictionary<string, string> _declaredQueues;

        public CustomRouteMessagePublisher(MessageChannelBase channel, string exchange = null, bool persistent = false)
            : base(channel, exchange, null, persistent)
        {
        }

        public override Task Publish(T message, Action<IBasicProperties> propertiesConfig = null)
        {
            throw new NotSupportedException();
        }

        public bool DeclareQueues
        {
            get => _declareQueues;
            set
            {
                if (value && _declaredQueues == null)
                {
                    _declaredQueues = new ConcurrentDictionary<string, string>();
                }

                _declareQueues = value;
            }
        }

        public async Task<bool> Publish(string routingKey, T message, Action<IBasicProperties> propertiesConfig = null)
        {
            EnsureQueue(routingKey);
            await base.Publish(Exchange, routingKey, message, propertiesConfig);
            return true;
        }

        public async Task<bool> Publish(string routingKey, T message, byte priority)
        {
            EnsureQueue(routingKey);
            await base.Publish(Exchange, routingKey, message, props => props.Priority = priority);
            return true;
        }

        private void EnsureQueue(string routingKey)
        {
            if (DeclareQueues && !_declaredQueues.ContainsKey(routingKey))
            {
                if (Channel.AutoDeclareQueue(Exchange, routingKey))
                {
                    _declaredQueues.TryAdd(routingKey, null);
                }
            }
        }
    }
}