using System;
using System.Threading.Tasks;
using RabbitMQ.Client.Events;

namespace NimbleRabbit
{
    public class ConsumeErrorPublisher : IErrorPublisher
    {
        private readonly MessageChannelBase _channel;
        private CustomRouteMessagePublisher<ConsumeError> _publisher;
        
        public ConsumeErrorPublisher(MessageChannelBase channel)
        {
            _channel = channel;
        }

        public async Task<bool> Publish(BasicDeliverEventArgs args, string queue, Exception exception, string stage)
        {
            if (_channel.IsClosed)
                return false;

            if (_publisher == null)
            {
                _publisher = new CustomRouteMessagePublisher<ConsumeError>(_channel, "", true)
                {
                    DeclareQueues = true
                };
            }

            string routingKey = (queue ?? args.RoutingKey) + ".error"; 
            try
            {
                await _publisher.Publish(
                    routingKey,
                    new ConsumeError(args, exception, stage));
            }
            catch (Exception e)
            {
                _channel.LogError(e, "Error publication exception {RoutingKey}", routingKey);
                return false;
            }
            
            return true;
        }
    }
}