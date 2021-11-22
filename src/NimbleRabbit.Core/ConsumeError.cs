using System;
using RabbitMQ.Client.Events;

namespace NimbleRabbit
{
    public class ConsumeError
    {
        //public IBasicProperties BasicProperties { get; set; }

        /// <summary>The message body.</summary>
        public byte[] Body { get; set; }

        /// <summary>The consumer tag of the consumer that the message
        /// was delivered to.</summary>
        public string ConsumerTag { get; set; }

        /// <summary>The delivery tag for this delivery. See
        /// IModel.BasicAck.</summary>
        public ulong DeliveryTag { get; set; }

        /// <summary>The exchange the message was originally published
        /// to.</summary>
        public string Exchange { get; set; }

        /// <summary>The AMQP "redelivered" flag.</summary>
        public bool Redelivered { get; set; }

        /// <summary>The routing key used when the message was
        /// originally published.</summary>
        public string RoutingKey { get; set; }

        public string Exception { get; set; }

        public string Stage { get; set; }

        public DateTimeOffset Timestamp { get; set; }

        public ConsumeError()
        {
        }

        public ConsumeError(BasicDeliverEventArgs args, Exception exception, string stage)
        {
            if (args == null) throw new ArgumentNullException(nameof(args));

            Body = args.Body;
            ConsumerTag = args.ConsumerTag;
            DeliveryTag = args.DeliveryTag;
            Exchange = args.Exchange;
            Redelivered = args.Redelivered;
            RoutingKey = args.RoutingKey;
            Exception = exception?.ToString();
            Stage = stage;
            Timestamp = DateTimeOffset.Now;
        }
    }
}