using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using RabbitMQ.Client;

namespace NimbleRabbit
{
    public static class CustomMessagePublisherExtensions
    {
        public static CustomMessagePublisher<T> Handle<T>(
            this CustomMessagePublisher<T> publisher, Action<MessagePublishArgs<T>> handler)
        {
            publisher.AddHandler(handler);
            return publisher;
        }

        public static CustomMessagePublisher<T> Route<T>(
            this CustomMessagePublisher<T> publisher, string exchange, string routingKey)
        {
            if (!string.IsNullOrEmpty(routingKey))
            {
                publisher.Channel.AutoDeclareQueue(exchange, routingKey);                
            }
            
            publisher.AddHandler(args =>
            {
                args.Exchange = exchange;
                args.RoutingKey = routingKey;
            });
            return publisher;
        }

        public static CustomMessagePublisher<T> Pack<T>(
            this CustomMessagePublisher<T> publisher, IDataPacker packer)
        {
            publisher.AddHandler(args => args.Body = packer.Pack(args.Body));
            return publisher;
        }

        public static CustomMessagePublisher<T> Persistent<T>(
            this CustomMessagePublisher<T> publisher)
        {
            publisher.AddHandler(args => args.Properties.Persistent = true);
            return publisher;
        }

        public static CustomMessagePublisher<T> Timestamp<T>(
            this CustomMessagePublisher<T> publisher)
        {
            publisher.AddHandler(args => args.Properties.Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()));
            return publisher;
        }

        public static CustomMessagePublisher<T> Serialize<T>(
            this CustomMessagePublisher<T> publisher, IMessageSerializer<T> serializer)
        {
            publisher.AddHandler(args => args.Body = serializer.Pack(args.Message));
            return publisher;
        }

        public static CustomMessagePublisher<T> DefaultSerializer<T>(
            this CustomMessagePublisher<T> publisher)
        {
            return Serialize(publisher, publisher.Channel.GetSerializer<T>());
        }

        public static CustomMessagePublisher<T> FixedTypeList<T>(
            this CustomMessagePublisher<T> publisher, IReadOnlyList<Type> messageTypes)
        {
            var serializers = new IMessageSerializer<object>[messageTypes.Count];
            for (var i = 0; i < messageTypes.Count; i++)
            {
                serializers[i] = publisher.Channel.GetSerializer(messageTypes[i]);
            }

            publisher.AddHandler(args =>
            {
                var type = args.Message.GetType();
                for (int i = 0; i < messageTypes.Count; i++)
                {
                    if (messageTypes[i] != type)
                        continue;

                    using (var ms = new MemoryStream())
                    {
                        ms.WriteByte((byte)i);
                        serializers[i].Pack(ms, args.Message);
                        args.Body = ms.ToArray();
                    }

                    return;
                }

                throw new InvalidOperationException("Unknown message type " + type.Name);
            });

            return publisher;
        }

        public static CustomMessagePublisher<T> DynamicQueueDeclare<T>(
            this CustomMessagePublisher<T> publisher)
        {
            var declaredQueues = new ConcurrentDictionary<string, string>();
            publisher.AddPostHandler(args =>
            {
                if (publisher.Channel.Settings.DeclareQueues &&
                    string.IsNullOrEmpty(args.Exchange) &&
                    !string.IsNullOrEmpty(args.RoutingKey) &&
                    !declaredQueues.ContainsKey(args.RoutingKey))
                {
                    publisher.Channel.DeclareQueue(args.RoutingKey, true);
                    declaredQueues.TryAdd(args.RoutingKey, null);
                }
            });
            return publisher;
        }

    }
}