using System;
using System.IO;
using System.Security.Cryptography;

namespace NimbleRabbit
{
    public class SymmetricMessageEncryptor : IExtendedDataPacker, IDisposable
    {
        private readonly SymmetricAlgorithm _algorithm;

        public SymmetricMessageEncryptor(SymmetricAlgorithm algorithm)
        {
            _algorithm = algorithm ?? throw new ArgumentNullException(nameof(algorithm));
        }

        public byte[] Pack(byte[] document)
        {
            using (var memoryStream = new MemoryStream())
            {
                using (var encryptor = _algorithm.CreateEncryptor(_algorithm.Key, _algorithm.IV))
                using (var cryptoStream = new CryptoStream(memoryStream, encryptor, CryptoStreamMode.Write))
                {
                    cryptoStream.Write(document, 0, document.Length);
                }

                return memoryStream.ToArray();
            }
        }

        public byte[] Pack(Action<Stream> writer)
        {
            using (var memoryStream = new MemoryStream())
            {
                using (var encryptor = _algorithm.CreateEncryptor(_algorithm.Key, _algorithm.IV))
                using (var cryptoStream = new CryptoStream(memoryStream, encryptor, CryptoStreamMode.Write))
                {
                    writer(cryptoStream);
                }

                return memoryStream.ToArray();
            }
        }

        public byte[] Unpack(byte[] data)
        {
            using (var decryptor = _algorithm.CreateDecryptor(_algorithm.Key, _algorithm.IV))
            using (var memoryStream = new MemoryStream(data))
            using (var resultStream = new MemoryStream())
            {
                using (var cryptoStream = new CryptoStream(memoryStream, decryptor, CryptoStreamMode.Read))
                {
                    cryptoStream.CopyTo(resultStream);
                }

                return resultStream.ToArray();
            }
        }

        public T Unpack<T>(byte[] data, Func<Stream, T> handler)
        {
            using (var memoryStream = new MemoryStream(data))
            using (var decryptor = _algorithm.CreateDecryptor(_algorithm.Key, _algorithm.IV))
            using (var cryptoStream = new CryptoStream(memoryStream, decryptor, CryptoStreamMode.Read))
            {
                return handler(cryptoStream);
            }
        }

        public void Dispose()
        {
            _algorithm?.Dispose();
        }
    }
}