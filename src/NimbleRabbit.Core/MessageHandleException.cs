using System;

namespace NimbleRabbit
{
    public class MessageHandleException : Exception
    {
        public MessageHandleException()
        {
        }

        public MessageHandleException(string message) : base(message)
        {
        }

        public MessageHandleException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}