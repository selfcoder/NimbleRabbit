using System;
using System.IO;

namespace NimbleRabbit
{
    public interface IMessageSerializer
    {
        
    }
    
    public interface IMessageSerializer<T>
    {
        byte[] Pack(T message);
        void Pack(Stream dest, T message);
        T Unpack(byte[] message);
        T Unpack(Stream stream);
    }

    public interface IMessageSerializerProvider
    {
        IMessageSerializer<object> GetSerializer(Type type);
        IMessageSerializer<T> GetSerializer<T>();
    }
}