using System;
using System.Threading.Tasks;
using RabbitMQ.Client.Events;

namespace NimbleRabbit
{
    public class CustomErrorPublisher : IErrorPublisher
    {
        private const string ConsumeErrorQueue = "consume.errors.net";

        private readonly MessageChannelBase _channel;
        private readonly string _routingKey;
        private MessagePublisher<ConsumeError> _publisher;

        public CustomErrorPublisher(MessageChannelBase channel, string routingKey = ConsumeErrorQueue)
        {
            _channel = channel;
            _routingKey = routingKey;
        }

        public async Task<bool> Publish(BasicDeliverEventArgs args, string queue, Exception exception, string stage)
        {
            if (_channel.IsClosed)
                return false;

            if (_publisher == null)
            {
                _publisher = _channel.CreatePublisher<ConsumeError>("", _routingKey, true);
            }

            try
            {
                await _publisher.Publish(new ConsumeError(args, exception, stage));
            }
            catch (Exception e)
            {
                _channel.LogError(e, "Error publication exception {RoutingKey}", _routingKey);
                return false;
            }

            return true;
        }
    }
}