using System;

namespace NimbleRabbit
{
    public class MessageBusSettings
    {
        public int ConnectRetry { get; set; }
        public TimeSpan ConnectRetryDelay { get; set; }
        public ushort Prefetch { get; set; }
        public bool DeclareQueues { get; set; }

        public MessageBusSettings()
        {
            ConnectRetryDelay = TimeSpan.FromSeconds(1);
            Prefetch = 20;
        }
    }
}