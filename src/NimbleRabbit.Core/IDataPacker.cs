using System;
using System.IO;

namespace NimbleRabbit
{
    public interface IDataPacker
    {
        byte[] Pack(byte[] data);
        byte[] Unpack(byte[] data);
    }

    public interface IDataPackerExtended
    {
        byte[] Pack(Action<Stream> writer);
        T Unpack<T>(byte[] data, Func<Stream, T> reader);
    }

    public interface IExtendedDataPacker : IDataPacker, IDataPackerExtended
    {
    }
}