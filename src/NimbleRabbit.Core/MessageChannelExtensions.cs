using System;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using RabbitMQ.Client.Events;

namespace NimbleRabbit
{
    public static class MessageChannelExtensions
    {
        public static IMessageSerializer<T> GetSerializer<T>(this MessageChannelBase channel)
        {
            var provider = channel.SerializerProvider;
            if (provider == null)
                throw new InvalidOperationException("Serializer provider does not initialized");

            var serializer = provider.GetSerializer<T>();
            if (serializer == null)
                throw new InvalidOperationException("Serializer provider does not return serializer");

            return serializer;
        }
        
        public static IMessageSerializer<object> GetSerializer(this MessageChannelBase channel, Type type)
        {
            var provider = channel.SerializerProvider;
            if (provider == null)
                throw new InvalidOperationException("Serializer provider does not initialized");

            var serializer = provider.GetSerializer(type);
            if (serializer == null)
                throw new InvalidOperationException("Serializer provider does not return serializer");

            return serializer;
        }
        
        public static IDisposable SubscribeManualAsync<T>(
            this MessageChannelBase channel, string queue, SubscriptionOptions options, Func<T, AckToken, Task> action)
        {
            var serializer = channel.GetSerializer<T>();
            return channel.SubscribeBaseAsync(queue, options, async (args) =>
            {
                T message;
                try
                {
                    message = serializer.Unpack(args.Body);
                }
                catch (Exception ex)
                {
                    if (await channel.PublishError(args, queue, ex, "Unpack"))
                    {
                        channel.Ack(args);
                    }
                    
                    return;
                }

                try
                {
                    await action(message, new AckToken(channel, args)).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    if (await channel.PublishError(args, queue, ex, "Handler"))
                    {
                        channel.Ack(args);
                    }
                }
            });
        }

        public static IDisposable SubscribeAsync(
            this MessageChannelBase channel, string queue, Func<BasicDeliverEventArgs, Task<bool>> action)
        {
            return SubscribeAsync(channel, queue, SubscriptionOptions.Default, action);
        }
        
        public static IDisposable SubscribeAsync(
            this MessageChannelBase channel, string queue, SubscriptionOptions options, Func<BasicDeliverEventArgs, Task<bool>> action)
        {
            return channel.SubscribeBaseAsync(queue, options, async (args) =>
            {
                bool ack;
                try
                {
                    ack = await action(args).ConfigureAwait(false);
                }
                catch (TaskCanceledException)
                {
                    return;
                }
                catch (Exception ex)
                {
                    channel.LogError(ex, "Message consume error on queue {Queue}", queue, args);
                    // Ack if error publishing has been succeeded
                    ack = await channel.PublishError(args, queue, ex, "Handler").ConfigureAwait(false);
                }

                if (ack)
                {
                    channel.Ack(args);
                }
                else
                {
                    channel.Nack(args, true);
                }
            });
        }

        public static IDisposable SubscribeDefaultAckAsync(
            this MessageChannelBase channel, string queue, Func<BasicDeliverEventArgs, Task> action)
        {
            return channel.SubscribeAsync(queue, async args =>
            {
                await action(args);
                return true;
            });
        }

        public static IDisposable SubscribeAsync<T>(
            this MessageChannelBase channel, string queue, Func<T, Task<bool>> action)
        {
            return SubscribeAsync<T>(channel, queue, SubscriptionOptions.Default, action);
        }
        
        public static IDisposable SubscribeAsync<T>(
            this MessageChannelBase channel, string queue, SubscriptionOptions options, Func<T, Task<bool>> action)
        {
            var serializer = channel.GetSerializer<T>();
            return channel.SubscribeAsync(queue, options, async (args) =>
            {
                var message = serializer.Unpack(args.Body);
                return await action(message);
            });
        }

        public static IDisposable SubscribeAsync<T>(
            this MessageChannelBase channel, string queue, Func<T, BasicDeliverEventArgs, Task<bool>> action)
        {
            var serializer = channel.GetSerializer<T>();
            return channel.SubscribeAsync(queue, async (args) =>
            {
                var message = serializer.Unpack(args.Body);
                return await action(message, args);
            });
        }
 
        public static IDisposable SubscribeAsync<T>(
            this MessageChannelBase channel,
            string queue,
            SubscriptionOptions options,
            IDataPacker packer,
            Func<T, Task<bool>> action)
        {
            var serializer = channel.GetSerializer<T>();
            return channel.SubscribeAsync(queue, options, async (args) =>
            {
                var data = packer.Unpack(args.Body);
                var message = serializer.Unpack(data);
                return await action(message);
            });
        }

        public static IDisposable SubscribeTypedAsync(
            this MessageChannelBase channel, string queue, Func<object, BasicDeliverEventArgs, Task<bool>> action)
        {
            return SubscribeTypedAsync(channel, queue, null, action);
        }

        public static IDisposable SubscribeTypedAsync(
            this MessageChannelBase channel, string queue, IDataPackerExtended dataPacker,
            Func<object, BasicDeliverEventArgs, Task<bool>> action)
        {
            return channel.SubscribeAsync(queue, args => HandleTypedMessage(channel, args, dataPacker, action));
        }

        private static async Task<bool> HandleTypedMessage(
            MessageChannelBase channel,
            BasicDeliverEventArgs args,
            IDataPackerExtended dataPacker,
            Func<object, BasicDeliverEventArgs, Task<bool>> action)
        {
            if (string.IsNullOrEmpty(args.BasicProperties.Type))
            {
                throw new MessageHandleException("Message type is undefined");
            }

            Type type = GetType(args.BasicProperties.Type);
            if (type == null)
            {
                throw new MessageHandleException($"Unknown message type '{args.BasicProperties.Type}'");
            }

            var serializer = channel.GetSerializer(type);
            object message;
            if (dataPacker != null)
            {
                message = dataPacker.Unpack(args.Body, stream => serializer.Unpack(stream));
            }
            else
            {
                message = serializer.Unpack(args.Body);
            }

            return await action(message, args);
        }

        public static MessagePublisher<T> CreatePublisher<T>(this MessageChannelBase channel, string exchange, string routingKey, bool persistent)
        {
            if (channel.Settings.DeclareQueues)
            {
                if (!string.IsNullOrEmpty(exchange))
                {
                    channel.DeclareExchange(exchange, persistent);
                }
                else
                {
                    channel.DeclareQueue(routingKey, persistent);
                }
            }

            return new MessagePublisher<T>(channel, exchange, routingKey, persistent);
        }

        private static Type GetType(string name)
        {
            return Type.GetType(name, ResolveAssembly, ResolveType, false);
        }

        private static Assembly ResolveAssembly(AssemblyName name)
        {
            //Assembly.GetEntryAssembly().GetReferencedAssemblies()
            try
            {
                return Assembly.Load(name);
            }
            catch
            {
                return Assembly.LoadFrom(Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location), name.Name + ".dll"));
            }
        }

        private static Type ResolveType(Assembly assembly, string typeName, bool ignoreCase)
        {
            return assembly.GetType(typeName, false, ignoreCase);
        }
    }
}