using System;
using System.IO;
using MsgPack.Serialization;

namespace NimbleRabbit
{
    public class MessagePackSerializerProvider : IMessageSerializerProvider
    {
        private readonly SerializationContext _context;

        public MessagePackSerializerProvider()
            : this(SerializationContext.Default)
        {
        }

        public MessagePackSerializerProvider(SerializationContext context)
        {
            _context = context;
        }

        public IMessageSerializer<object> GetSerializer(Type type)
        {
            return new MsgPackSerializer(_context.GetSerializer(type));
        }

        public IMessageSerializer<T> GetSerializer<T>()
        {
            return new MsgPackSerializer<T>(_context.GetSerializer<T>());
        }
    }
    
    public class MsgPackSerializer : IMessageSerializer<object>
    {
        private readonly MessagePackSerializer _serializer;

        public MsgPackSerializer(MessagePackSerializer serializer)
        {
            _serializer = serializer;
        }

        public byte[] Pack(object message)
        {
            return _serializer.PackSingleObject(message);
        }

        public void Pack(Stream dest, object message)
        {
            _serializer.Pack(dest, message);
        }

        public object Unpack(byte[] message)
        {
            return _serializer.UnpackSingleObject(message);
        }

        public object Unpack(Stream stream)
        {
            return _serializer.Unpack(stream);
        }
    }

    public class MsgPackSerializer<T> : IMessageSerializer<T>
    {
        private readonly MessagePackSerializer<T> _serializer;

        public MsgPackSerializer(MessagePackSerializer<T> serializer)
        {
            _serializer = serializer;
        }

        public byte[] Pack(T message)
        {
            return _serializer.Pack(message);
        }

        public void Pack(Stream dest, T message)
        {
            _serializer.Pack(dest, message);
        }

        public T Unpack(byte[] message)
        {
            return _serializer.Unpack(message);
        }

        public T Unpack(Stream stream)
        {
            return _serializer.Unpack(stream);
        }
    }
}