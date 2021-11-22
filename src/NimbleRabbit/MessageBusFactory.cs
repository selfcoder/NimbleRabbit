using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace NimbleRabbit
{
    public static class MessageBusFactory
    {
        public static MessageBusConnection CreateBus(
            IConfiguration configuration, ITaskDispatcher taskDispatcher, ILogger log, string name = null)
        {
            var factory = new ConnectionFactory();
            configuration.Bind(factory);
            var settings = configuration.Get<MessageBusSettings>();
            return new MessageBusConnection(factory, settings, taskDispatcher, new MessagePackSerializerProvider(), log, name);
        }
    }
}
