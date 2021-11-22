using System;
using System.Threading.Tasks;
using RabbitMQ.Client.Events;

namespace NimbleRabbit
{
    public class AckToken
    {
        private readonly MessageChannelBase _channel;
        private readonly BasicDeliverEventArgs _args;

        public AckToken(MessageChannelBase channel, BasicDeliverEventArgs args)
        {
            _channel = channel;
            _args = args;
        }

        public void Ack()
        {
            _channel.Ack(_args);
        }

        public void Nack(bool requeue)
        {
            _channel.Nack(_args, requeue);
        }

        public async Task<bool> PublishError(Exception ex, string queue, string stage, bool ackOrNack)
        {
            var published = await _channel.PublishError(_args, queue, ex, stage);
            if (ackOrNack)
            {
                if (published)
                {
                    Ack();
                }
                else
                {
                    Nack(true);
                }
            }

            return published;
        }
    }
}