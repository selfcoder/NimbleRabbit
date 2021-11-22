using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
//using Serilog;

namespace NimbleRabbit
{
    public class MessageBusConnection : IDisposable
    {
        private readonly ConnectionFactory _factory;
        private readonly MessageBusSettings _settings;
        private readonly IMessageSerializerProvider _serializerProvider;
        private readonly ILogger _log;
        private readonly string _name;
        private IConnection _connection;
        private ITaskDispatcher _dispatcher;
        private bool _closed;

        public MessageBusConnection(
            ConnectionFactory factory,
            MessageBusSettings settings,
            ITaskDispatcher taskDispatcher,
            IMessageSerializerProvider serializerProvider,
            ILogger log,
            string name = null)
        {
            _factory = factory ?? throw new ArgumentNullException(nameof(factory));
            _settings = settings ?? throw new ArgumentNullException(nameof(settings));
            _dispatcher = taskDispatcher ?? throw new ArgumentNullException(nameof(taskDispatcher));
            _serializerProvider = serializerProvider;
            _log = log ?? throw new ArgumentNullException(nameof(log));
            _name = name;

#if !DEBUG
            if (_settings.DeclareQueues)
            {
                _log.Warning("MessageBus: queue declaring is enabled");
            }
#endif
        }

        public IConnection Connection
        {
            get
            {
                if (_connection == null)
                    throw new InvalidOperationException("Connection is not opened");

                return _connection;
            }
        }

        public ITaskDispatcher Dispatcher
        {
            get => _dispatcher;
            set => _dispatcher = value ?? throw new ArgumentNullException();
        }

        public ILogger Logger => _log;

        public MessageChannelBase CreateChannel(
            IErrorPublisher errorPublisher = null,
            IMessageSerializerProvider serializerProvider = null,
            ILogger logger = null)
        {
            return new MsLogMessageChannel(
                Connection,
                _settings,
                _dispatcher,
                errorPublisher,
                serializerProvider ?? _serializerProvider,
                logger ?? _log);
        }

        public void Close()
        {
            _connection?.ConsumerWorkService.StopWork();
            _connection?.Close();
            _closed = true;
        }

        public void Dispose()
        {
            if (!_closed)
            {
                Close();
            }
            
            _connection?.Dispose();
        }

        public Task OpenAsync(CancellationToken cancellationToken)
        {
            return Task.Run(async () =>
            {
                int attempt = 0;
                Exception exception = null;
                string name = _name ?? _factory.Endpoint.ToString();
                while (!cancellationToken.IsCancellationRequested &&
                    (_settings.ConnectRetry <= 0 || attempt < _settings.ConnectRetry))
                {
                    if (attempt > 0 && _settings.ConnectRetryDelay.Ticks > 0)
                    {
                        await Task.Delay(_settings.ConnectRetryDelay, cancellationToken);
                    }
                    
                    try
                    {
                        _log.LogInformation("Connecting to RabbitMQ {Name}", name);
                        _connection = _factory.CreateConnection();
                        _log.LogInformation("Connection {Name} has been established successfully", name);
                        _connection.ConnectionShutdown += (sender, args) =>
                        {
                            var ex = args.Cause as Exception;
                            if (ex != null)
                            {
                                _log.LogError(ex, "Connection {Name} shutdown (reply: {ReplyText})", name, args.ReplyText);
                            }
                            else if (args.Initiator == ShutdownInitiator.Application)
                            {
                                _log.LogInformation("Connection {Name} shutdown (reply: {ReplyText})", name, args.ReplyText);
                            }
                            else
                            {
                                _log.LogWarning("Connection {Name} shutdown (reply: {ReplyText})", name, args.ReplyText);
                            }
                        };
                        _connection.RecoverySucceeded += (sender, args) =>
                        {
                            _log.LogInformation("Connection {Name} recovery succeeded", name);
                        };
                        return;
                    }
                    catch (BrokerUnreachableException ex)
                    {
                        exception = ex;
                        _log.LogError("Connection to {Name} failed: {Error}", name, ex.Message);
                    }

                    attempt++;
                }

                if (_connection == null && exception != null)
                {
                    throw exception;
                }
            }, cancellationToken);
        }
    }
}