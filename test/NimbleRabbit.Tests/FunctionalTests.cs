using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Moq;
using Nito.AsyncEx;
using RabbitMQ.Client;
using Xunit;

namespace NimbleRabbit.Tests
{
    public record Foo
    {
        public string Text { get; set; }
        public int Number { get; set; }
    }

    public record FooMessage : Foo
    {
        public string Id { get; set; }
        public string Sender { get; set; }
    }

    public record ReceivedMessage<T>(T Message, int ThreadId)
    {
    }
    
    public class FunctionalTests : RabbitMQFixture
    {
        private static async Task<MessageBusConnection> CreateAndOpenConnection(TaskDispatcher taskDispatcher = null)
        {
            var settings = new MessageBusSettings
            {
                ConnectRetry = 15,
                DeclareQueues = true
            };

            if (taskDispatcher == null)
            {
                taskDispatcher = new TaskDispatcher();
                // taskDispatcher.MaxThreads = 1;
                // taskDispatcher.MaxTasksPerThread = 1;
            }

            var logger = Mock.Of<ILogger>();
            var connection = new MessageBusConnection(
                new ConnectionFactory(),
                settings,
                taskDispatcher,
                new MessagePackSerializerProvider(),
                logger);

            await connection.OpenAsync(CancellationTokenHelper.TimeoutSeconds(15));
            return connection;
        }
        
        private static string GenerateQueueName()
        {
            return "test-" + Guid.NewGuid().ToString().Substring(0, 6);
        }
        
        [Fact]
        public async void ConnectionOpenAndClose()
        {
            using (var connection = await CreateAndOpenConnection())
            {
            }
        }
        
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async void PublishTypedMessage(bool persistent)
        {
            Foo sentMessage = new Foo { Text = "abc" };
            Foo receivedMessage = null;

            string queue = GenerateQueueName();
            using (var connection = await CreateAndOpenConnection())
            {
                var channel = connection.CreateChannel();
                var publisher = channel.CreatePublisher<Foo>("", queue, persistent);

                await publisher.Publish(sentMessage);

                var receivedEvent = new AsyncManualResetEvent();
                channel.SubscribeAsync<Foo>(queue, SubscriptionOptions.NoDeclare, async f =>
                {
                    receivedMessage = f;
                    receivedEvent.Set();
                    return true;
                });

                await receivedEvent.WaitAsync(CancellationTokenHelper.TimeoutSeconds(5));
            }
            
            Assert.Equal(sentMessage, receivedMessage);
        }

        [Fact]
        public async void NotSerializedMessageWithId()
        {
            byte[] receivedMessage = null;
            string receivedMessageId = null;

            string queue = GenerateQueueName();
            using (var connection = await CreateAndOpenConnection())
            {
                var channel = connection.CreateChannel();
                channel.DeclareQueue(queue, false);
                
                var publisher = new MessagePublisher(channel, "", queue, false);

                await publisher.Publish(new byte[] {1, 2, 3}, properties => properties.MessageId = "id123");

                var receivedEvent = new AsyncManualResetEvent();
                channel.SubscribeAsync(queue, SubscriptionOptions.NoDeclare, args =>
                {
                    receivedMessage = args.Body;
                    receivedMessageId = args.BasicProperties.MessageId;
                    receivedEvent.Set();
                    return Task.FromResult(true);
                });

                await receivedEvent.WaitAsync(CancellationTokenHelper.TimeoutSeconds(5));
            }
            
            Assert.Equal(new byte[] {1, 2, 3}, receivedMessage);
            Assert.Equal("id123", receivedMessageId);
        }

        [Theory]
        [InlineData(10, 2)]
        [InlineData(100, 2)]
        [InlineData(1000, 5)]
        public async void BulkPublishTypedMessage(int count, int threadCount)
        {
            var receivedMessages = new ConcurrentBag<ReceivedMessage<Foo>>();

            string queue = GenerateQueueName();
            var dispatcher = new TaskDispatcher { MaxThreads = threadCount };
            using (var connection = await CreateAndOpenConnection(dispatcher))
            {
                var channel = connection.CreateChannel();
                var publisher = channel.CreatePublisher<Foo>("", queue, false);

                for (int i = 0; i < count; i++)
                {
                    var message = new Foo { Number = i, Text = "abc" };
                    await publisher.Publish(message);
                }

                var receivedEvent = new AsyncManualResetEvent();
                channel.SubscribeAsync<Foo>(queue, SubscriptionOptions.NoDeclare, async f =>
                {
                    receivedMessages.Add(new ReceivedMessage<Foo>(f, Thread.CurrentThread.ManagedThreadId));

                    if (receivedMessages.Count >= count)
                    {
                        receivedEvent.Set();                        
                    }

                    return true;
                });

                await receivedEvent.WaitAsync(CancellationTokenHelper.TimeoutSeconds(5 + (int)(count * 0.01)));
            }

            Assert.Equal(count, receivedMessages.Count);
            //int actualThreadCount = receivedMessages.Select(m => m.ThreadId).Distinct().Count();
            int actualThreadCount = receivedMessages.GroupBy(m => m.ThreadId).Count();
            Assert.Equal(threadCount, actualThreadCount);
        }
        
        
        [Fact]
        public async void ManualNack()
        {
            int handled = 0;
            
            string queue = GenerateQueueName();
            using (var connection = await CreateAndOpenConnection())
            {
                var channel = connection.CreateChannel();
                var publisher = channel.CreatePublisher<Foo>("", queue, false);

                await publisher.Publish(new Foo { Text = "abc" });

                channel.SubscribeManualAsync<Foo>(queue, SubscriptionOptions.NoDeclare, async (f, token) =>
                {
                    handled++;
                    if (handled == 1)
                    {
                        token.Nack(true);                        
                    }
                    else
                    {
                        token.Nack(false);
                    }
                });

                await Task.Delay(5 * 1000);
            }
            
            Assert.Equal(2, handled);
        }

        [Fact]
        public async void CustomRoutes()
        {
            var receivedMessages = new ConcurrentBag<byte[]>();
            string queue1 = GenerateQueueName();
            string queue2 = GenerateQueueName();
            using (var connection = await CreateAndOpenConnection())
            {
                var channel = connection.CreateChannel();
                
                channel.SubscribeAsync(queue1, args =>
                {
                    receivedMessages.Add(args.Body);
                    return Task.FromResult(true);
                });
                
                channel.SubscribeAsync(queue2, args =>
                {
                    receivedMessages.Add(args.Body);
                    return Task.FromResult(true);
                });
                
                var publisher = new CustomRouteMessagePublisher(channel);

                await Task.WhenAll(
                    publisher.Publish(queue1, new byte[] {0, 1}),
                    publisher.Publish(queue2, new byte[] {0, 2}));
                
                await Task.Delay(5 * 1000);
            }
            
            Assert.Equal(2, receivedMessages.Count);
            Assert.Contains(new byte[] {0, 1}, receivedMessages);
            Assert.Contains(new byte[] {0, 2}, receivedMessages);
        }

        [Fact]
        public async void CustomPublisher()
        {
            FooMessage receivedMessage = null;
            IBasicProperties receivedMessageProps = null;
            
            string queue = GenerateQueueName();
            using (var connection = await CreateAndOpenConnection())
            {
                var channel = connection.CreateChannel();

                var receivedEvent = new AsyncManualResetEvent();
                channel.SubscribeAsync<FooMessage>(queue, (f, args) =>
                {
                    receivedMessage = f;
                    receivedMessageProps = args.BasicProperties;
                    receivedEvent.Set();
                    return Task.FromResult(true);;
                });

                var publisher = new CustomMessagePublisher<FooMessage>(channel)
                    .Route("", queue)
                    .Persistent()
                    .Handle(m =>
                    {
                        m.Properties.MessageId = m.Message.Id;
                        m.Properties.ReplyTo = m.Message.Sender;
                    });

                await publisher.Publish(new FooMessage
                {
                    Id = "123",
                    Number = 10,
                    Sender = "r2d2"
                });
                
                await receivedEvent.WaitAsync(CancellationTokenHelper.TimeoutSeconds(5));
            }

            Assert.NotNull(receivedMessage);
            Assert.Equal("123", receivedMessage.Id);
            Assert.Equal(10, receivedMessage.Number);
            
            Assert.NotNull(receivedMessageProps);
            Assert.Equal("123", receivedMessageProps.MessageId);
            Assert.Equal("r2d2", receivedMessageProps.ReplyTo);
            Assert.True(receivedMessageProps.Persistent);
        }

        [Fact]
        public async void TypedMessageWithEncryption()
        {
            Foo receivedMessage = null;
            
            string queue = GenerateQueueName();
            using (var myAes = System.Security.Cryptography.Aes.Create())
            using (var connection = await CreateAndOpenConnection())
            {
                var channel = connection.CreateChannel();
                var encryptor = new SymmetricMessageEncryptor(myAes);
                var receivedEvent = new AsyncManualResetEvent();
                channel.SubscribeAsync<Foo>(queue, SubscriptionOptions.Default, encryptor, f =>
                {
                    receivedMessage = f;
                    receivedEvent.Set();
                    return Task.FromResult(true);
                });

                var publisher = new CustomMessagePublisher<Foo>(channel)
                    .Route("", queue)
                    .Pack(encryptor);

                await publisher.Publish(new Foo
                {
                    Number = 10
                });
                
                await receivedEvent.WaitAsync(CancellationTokenHelper.TimeoutSeconds(5));
            }

            Assert.NotNull(receivedMessage);
            Assert.Equal(10, receivedMessage.Number);
        }
        
        [Fact]
        public async void ErrorHandling()
        {
            Foo sentMessage = new Foo { Text = "abc" };
            Foo receivedMessage = null;
            ConsumeError errorMessage = null;

            string queue = GenerateQueueName();
            using (var connection = await CreateAndOpenConnection())
            {
                var channel = connection.CreateChannel();
                channel.ErrorPublisher = new ConsumeErrorPublisher(channel);

                var receivedEvent = new AsyncManualResetEvent();
                channel.SubscribeAsync<Foo>(queue, f =>
                {
                    receivedMessage = f;
                    throw new ApplicationException("TEST");
                    receivedEvent.Set();
                });
                
                var errorEvent = new AsyncManualResetEvent();
                channel.SubscribeAsync<ConsumeError>(queue + ".error", async e =>
                {
                    errorMessage = e;
                    errorEvent.Set();
                    return true;
                });

                var publisher = channel.CreatePublisher<Foo>("", queue, true);
                await publisher.Publish(sentMessage);

                await Task.WhenAny(
                    receivedEvent.WaitAsync(CancellationTokenHelper.TimeoutSeconds(15)),
                    errorEvent.WaitAsync(CancellationTokenHelper.TimeoutSeconds(10)));
            }
            
            Assert.NotNull(errorMessage);
            Assert.Equal(queue, errorMessage.RoutingKey);
            Assert.StartsWith("System.ApplicationException: TEST", errorMessage.Exception);
        }
    }
}