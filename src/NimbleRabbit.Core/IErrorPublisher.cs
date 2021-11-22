using System;
using System.Threading.Tasks;
using RabbitMQ.Client.Events;

namespace NimbleRabbit
{
    public interface IErrorPublisher
    {
        Task<bool> Publish(BasicDeliverEventArgs args, string queue, Exception exception, string stage);
    }
}