using System;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace NimbleRabbit
{
    public class TaskDispatcherConsumer : DefaultBasicConsumer
    {
        private readonly ITaskDispatcher _taskDispatcher;
        private readonly Func<BasicDeliverEventArgs, Task> _action;

        public TaskDispatcherConsumer(IModel channel, ITaskDispatcher taskDispatcher, Func<BasicDeliverEventArgs, Task> action)
            : base(channel)
        {
            _taskDispatcher = taskDispatcher;
            _action = action;
        }

        public override void HandleBasicDeliver(string consumerTag, ulong deliveryTag, bool redelivered, string exchange, string routingKey, IBasicProperties properties, byte[] body)
        {
            base.HandleBasicDeliver(consumerTag, deliveryTag, redelivered, exchange, routingKey, properties, body);
            var args = new BasicDeliverEventArgs(
                consumerTag, deliveryTag, redelivered, exchange, routingKey, properties, body);
            if (!_taskDispatcher.TrySchedule(() => _action(args)) && Model.IsOpen)
            {
                lock (Model)
                {
                    Model.BasicNack(deliveryTag, false, true);
                }
            }
        }
    }
}