using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using RabbitMQ.Client.Events;

namespace NimbleRabbit
{
    public static class FixedTypeListMessageChannelExtensions
    {
        public static IDisposable SubscribeFixedTypesAsync(
            this MessageChannelBase channel,
            string queue,
            IDataPacker dataPacker,
            IReadOnlyList<Type> messageTypes,
            Func<object, BasicDeliverEventArgs, Task<bool>> action)
        {
            var serializers = new IMessageSerializer<object>[messageTypes.Count];
            for (var i = 0; i < messageTypes.Count; i++)
            {
                serializers[i] = channel.GetSerializer(messageTypes[i]);
            }

            return channel.SubscribeAsync(queue, args =>
            {
                var data = args.Body;
                if (dataPacker != null)
                {
                    data = dataPacker.Unpack(data);
                }

                object message;
                using (var stream = new MemoryStream(data, false))
                {
                    int typeIndex = stream.ReadByte();
                    var serializer = serializers[typeIndex];
                    message = serializer.Unpack(stream);
                }

                return action(message, args);
            });
        }
    }
}