using System;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace NimbleRabbit
{
    public class MsLogMessageChannel : MessageChannelBase
    {
        private readonly ILogger _logger;

        public MsLogMessageChannel(
            IConnection connection,
            MessageBusSettings settings,
            ITaskDispatcher taskDispatcher,
            IErrorPublisher errorPublisher = null,
            IMessageSerializerProvider serializerProvider = null,
            ILogger logger = null)
            : base(connection, settings, taskDispatcher, errorPublisher, serializerProvider)
        {
            _logger = logger;
        }

        public override void LogError(Exception exception, string message, params object[] args)
        {
            _logger?.LogError(exception, message, args);
        }
    }
}