using System;
using System.Collections.Generic;
using System.IO;

namespace NimbleRabbit
{
    public class FixedTypeListMessagePublisher : MessagePublisherBase<object>
    {
        private readonly IDataPackerExtended _dataPacker;
        private readonly IReadOnlyList<Type> _messageTypes;
        private readonly IMessageSerializer<object>[] _serializers;

        public FixedTypeListMessagePublisher(
            MessageChannelBase channel,
            IReadOnlyList<Type> messageTypes,
            IDataPackerExtended dataPacker,
            string exchange = null,
            string routingKey = null,
            bool persistent = false)
            : base(channel, exchange, routingKey, persistent)
        {
            _messageTypes = messageTypes;
            _dataPacker = dataPacker;
            _serializers = new IMessageSerializer<object>[messageTypes.Count];
            for (var i = 0; i < messageTypes.Count; i++)
            {
                _serializers[i] = channel.GetSerializer(messageTypes[i]);
            }
        }

        protected override byte[] Pack(object message)
        {
            var serializer = GetSerializer(message.GetType(), out byte typeIndex);
            if (_dataPacker != null)
            {
                return _dataPacker.Pack(s =>
                {
                    s.WriteByte(typeIndex);
                    serializer.Pack(s, message);
                });
            }

            using (var ms = new MemoryStream())
            {
                ms.WriteByte(typeIndex);
                serializer.Pack(ms, message);
                return ms.ToArray();
            }
        }

        private IMessageSerializer<object> GetSerializer(Type type, out byte index)
        {
            for (int i = 0; i < _messageTypes.Count; i++)
            {
                if (_messageTypes[i] == type)
                {
                    index = (byte)i;
                    return _serializers[i];
                }
            }

            throw new ArgumentOutOfRangeException(nameof(type), "Unknown type " + type.Name);
        }
    }
}