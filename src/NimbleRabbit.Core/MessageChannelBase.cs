using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace NimbleRabbit
{
    public class MessageChannelBase : IDisposable
    {
        public static IErrorPublisher DefaultErrorPublisher { get; set; }
        
        public IMessageSerializerProvider SerializerProvider { get; }
        
        private readonly IModel _model;
        private readonly object _modelLock;
        private readonly MessageBusSettings _settings;
        private readonly ITaskDispatcher _taskDispatcher;
        private readonly ConcurrentDictionary<ulong, TaskCompletionSource<object>> _confirms;
        private IErrorPublisher _errorPublisher;

        public MessageChannelBase(
            IConnection connection,
            MessageBusSettings settings,
            ITaskDispatcher taskDispatcher,
            IErrorPublisher errorPublisher = null,
            IMessageSerializerProvider serializerProvider = null)
        {
            SerializerProvider = serializerProvider;
            _settings = settings;
            _taskDispatcher = taskDispatcher;
            _errorPublisher = errorPublisher ?? DefaultErrorPublisher;
            _modelLock = new object();
            _model = connection.CreateModel();
            _model.BasicQos(0, settings.Prefetch, false);
            _model.ConfirmSelect();
            _model.BasicAcks += ModelOnAcks;
            _model.BasicNacks += ModelOnNacks;
            _model.ModelShutdown += ModelOnShutdown;
            _confirms = new ConcurrentDictionary<ulong, TaskCompletionSource<object>>();
        }

        public IErrorPublisher ErrorPublisher
        {
            get => _errorPublisher;
            set => _errorPublisher = value;
        }

        // ReSharper disable once InconsistentlySynchronizedField
        public bool IsClosed => _model.IsClosed;

        public MessageBusSettings Settings => _settings;

        public event EventHandler<ShutdownEventArgs> Shutdown
        {
            // ReSharper disable InconsistentlySynchronizedField
            add => _model.ModelShutdown += value;
            remove => _model.ModelShutdown -= value;
            // ReSharper restore InconsistentlySynchronizedField
        }

        public void Ack(BasicDeliverEventArgs args)
        {
            lock (_modelLock)
            {
                _model.BasicAck(args.DeliveryTag, false);
            }
        }

        public void Nack(BasicDeliverEventArgs args, bool requeue)
        {
            lock (_modelLock)
            {
                _model.BasicNack(args.DeliveryTag, false, requeue);
            }
        }

        public void DeclareExchange(string name, bool persistent)
        {
            lock (_modelLock)
            {
                _model.ExchangeDeclare(name,
                    type: ExchangeType.Fanout,
                    durable: persistent,
                    autoDelete: false,
                    arguments: null);
            }
        }

        public void DeclareQueue(string queue, bool persistent, string bindToExchange = null, IDictionary<string, object> arguments = null)
        {
            if (string.IsNullOrEmpty(queue))
                throw new ArgumentNullException(nameof(queue));
            
            lock (_modelLock)
            {
                _model.QueueDeclare(queue,
                    durable: persistent,
                    exclusive: false,
                    autoDelete: false,
                    arguments: arguments);

                if (!string.IsNullOrEmpty(bindToExchange))
                {
                    _model.QueueBind(queue, bindToExchange, "", null);
                }
            }
        }

        public MessagePublishArgs<T> CreatePublishArgs<T>(T message)
        {
            // ReSharper disable once InconsistentlySynchronizedField
            return new MessagePublishArgs<T>(message, _model.CreateBasicProperties());
        }

        public void Publish<T>(MessagePublishArgs<T> args)
        {
            lock (_modelLock)
            {
                _model.BasicPublish(
                    exchange: args.Exchange,
                    routingKey: args.RoutingKey,
                    mandatory: true,
                    basicProperties: args.Properties,
                    body: args.Body);
                _model.WaitForConfirmsOrDie();
            }
        }

        public Task PublishAsync<T>(MessagePublishArgs<T> args)
        {
            var taskSource = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            lock (_modelLock)
            {
                ulong deliveryTag = _model.NextPublishSeqNo;
                _confirms.TryAdd(deliveryTag, taskSource);
                try
                {
                    _model.BasicPublish(
                        exchange: args.Exchange,
                        routingKey: args.RoutingKey,
                        mandatory: true,
                        basicProperties: args.Properties,
                        body: args.Body);
                }
                catch (Exception ex)
                {
                    _confirms.TryRemove(deliveryTag, out _);
                    return Task.FromException(ex);
                }
            }

            return taskSource.Task;
        }

        public void Publish(string exchange, string routingKey, byte[] body, Action<IBasicProperties> propertiesConfig = null)
        {
            // ReSharper disable once InconsistentlySynchronizedField
            IBasicProperties properties = _model.CreateBasicProperties();
            propertiesConfig?.Invoke(properties);

            lock (_modelLock)
            {
                _model.BasicPublish(
                    exchange: exchange,
                    routingKey: routingKey,
                    mandatory: true,
                    basicProperties: properties,
                    body: body);
            }
        }

        public Task PublishAsync(string exchange, string routingKey, byte[] body,
            Action<IBasicProperties> propertiesConfig = null)
        {
            // ReSharper disable once InconsistentlySynchronizedField
            IBasicProperties properties = _model.CreateBasicProperties();
            propertiesConfig?.Invoke(properties);

            var taskSource = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            lock (_modelLock)
            {
                ulong deliveryTag = _model.NextPublishSeqNo;
                _confirms.TryAdd(deliveryTag, taskSource);
                try
                {
                    _model.BasicPublish(
                        exchange: exchange,
                        routingKey: routingKey,
                        mandatory: true,
                        basicProperties: properties,
                        body: body);
                }
                catch (Exception ex)
                {
                    _confirms.TryRemove(deliveryTag, out _);
                    return Task.FromException(ex);
                }
            }

            return taskSource.Task;
        }


        public IDisposable SubscribeBaseAsync(string queue, SubscriptionOptions options, Func<BasicDeliverEventArgs, Task> action)
        {
            if (!options.HasFlag(SubscriptionOptions.NoDeclare))
            {
                AutoDeclareQueue("", queue);
            }

            // ReSharper disable once InconsistentlySynchronizedField
            var consumer = new TaskDispatcherConsumer(_model, _taskDispatcher, action);
            string consumerTag;
            lock (_modelLock)
            {
                consumerTag = _model.BasicConsume(
                    queue, options.HasFlag(SubscriptionOptions.AutoAck), consumer);
            }

            // ReSharper disable once InconsistentlySynchronizedField
            return new ConsumerCancel(_model, _modelLock, consumerTag);
        }

        public bool AutoDeclareQueue(string exchange, string queue)
        {
            if (_settings.DeclareQueues)
            {
                DeclareQueue(queue, true);
                return true;
            }

            return false;
        }

        public Task<bool> PublishError(BasicDeliverEventArgs args, string queue, Exception exception, string stage)
        {
            var pub = _errorPublisher;
            if (pub != null)
            {
                return pub.Publish(args, queue, exception, stage);
            }

            return Task.FromResult(false);
        }

        public uint GetMessageCount(string queue)
        {
            uint count;
            lock (_modelLock)
            {
                count = _model.MessageCount(queue);
            }

            return count;
        }

        public void Dispose()
        {
            _model?.Dispose();
        }

        private void ModelOnAcks(object sender, BasicAckEventArgs e)
        {
            Confirm(e.DeliveryTag, e.Multiple, s => s.TrySetResult(null));
        }

        private void ModelOnNacks(object sender, BasicNackEventArgs e)
        {
            Confirm(e.DeliveryTag, e.Multiple, s => s.TrySetException(new MessageNotConfirmedException()));
        }

        private void Confirm(ulong deliveryTag, bool multiple, Action<TaskCompletionSource<object>> action)
        {
            if (!multiple)
            {
                if (_confirms.TryRemove(deliveryTag, out var tcs))
                {
                    action(tcs);
                }
            }
            else
            {
                ulong[] keys = _confirms.Keys.Where(k => k <= deliveryTag).ToArray();
                foreach (var key in keys)
                {
                    if (_confirms.TryRemove(key, out var tcs))
                    {
                        action(tcs);
                    }
                }
            }
        }

        private void ModelOnShutdown(object sender, ShutdownEventArgs e)
        {
            if (_confirms.Count > 0)
            {
                foreach (var pair in _confirms)
                {
                    pair.Value.SetException(new MessageNotConfirmedException());
                }

                _confirms.Clear();
            }
        }

        private class ConsumerCancel : IDisposable
        {
            private readonly IModel _model;
            private readonly object _modelLock;
            private string _consumerTag;

            public ConsumerCancel(IModel model, object modelLock, string consumerTag)
            {
                _model = model;
                _modelLock = modelLock;
                _consumerTag = consumerTag;
            }

            public void Dispose()
            {
                if (_consumerTag == null || _model.IsClosed)
                    return;

                lock (_modelLock)
                {
                    if (_consumerTag != null && !_model.IsClosed)
                    {
                        _model.BasicCancel(_consumerTag);
                        _consumerTag = null;
                    }
                }
            }
        }

        public virtual void LogError(Exception exception, string message, params object[] args)
        {
        }
    }

    [Flags]
    public enum SubscriptionOptions
    {
        None = 0,
        Default = None,
        AutoAck = 1 << 0,
        NoDeclare = 1 << 1
    }

    public class MessageNotConfirmedException : Exception
    {
    }
}