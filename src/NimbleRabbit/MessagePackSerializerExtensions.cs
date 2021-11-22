using System.IO;
using System.Threading.Tasks;
using MsgPack.Serialization;

namespace NimbleRabbit
{
    public static class MessagePackSerializerExtensions
    {
        public static byte[] Pack<T>(this MessagePackSerializer<T> serializer, T obj)
        {
            using (var ms = new MemoryStream())
            {
                serializer.Pack(ms, obj);
                return ms.ToArray();
            }
        }

        public static async Task<byte[]> PackAsync<T>(this MessagePackSerializer<T> serializer, T obj)
        {
            using (var ms = new MemoryStream())
            {
                await serializer.PackAsync(ms, obj);
                return ms.ToArray();
            }
        }

        public static T Unpack<T>(this MessagePackSerializer<T> serializer, byte[] data)
        {
            using (var ms = new MemoryStream(data))
            {
                return serializer.Unpack(ms);
            }
        }

        public static Task<T> UnpackAsync<T>(this MessagePackSerializer<T> serializer, byte[] data)
        {
            using (var ms = new MemoryStream(data))
            {
                return serializer.UnpackAsync(ms);
            }
        }
    }
}