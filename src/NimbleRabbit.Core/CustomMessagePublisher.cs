using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace NimbleRabbit
{
    public class CustomMessagePublisher<T> : IMessagePublisher<T>
    {
        private readonly MessageChannelBase _channel;
        private readonly IMessageSerializer<T> _serializer;
        private List<Action<MessagePublishArgs<T>>> _handlers;
        private List<Action<MessagePublishArgs<T>>> _postHandlers;

        public CustomMessagePublisher(MessageChannelBase channel)
        {
            _channel = channel ?? throw new ArgumentNullException(nameof(channel));
            if (typeof(T) != typeof(byte[]))
            {
                _serializer = channel.GetSerializer<T>();
            }
        }

        public MessageChannelBase Channel => _channel;

        public Task Publish(T message, Action<MessagePublishArgs<T>> handler = null)
        {
            MessagePublishArgs<T> args = _channel.CreatePublishArgs(message);
            if (_serializer != null)
            {
                args.Body = _serializer.Pack(message);
            }

            handler?.Invoke(args);

            if (_handlers != null)
            {
                for (int i = 0; i < _handlers.Count; i++)
                {
                    _handlers[i](args);
                }
            }

            if (_postHandlers != null)
            {
                for (int i = 0; i < _postHandlers.Count; i++)
                {
                    _postHandlers[i](args);
                }
            }

            return _channel.PublishAsync(args);
        }

        public void AddHandler(Action<MessagePublishArgs<T>> handler)
        {
            if (handler == null) throw new ArgumentNullException(nameof(handler));

            if (_handlers == null)
            {
                _handlers = new List<Action<MessagePublishArgs<T>>>();
            }

            _handlers.Add(handler);
        }

        public void AddPostHandler(Action<MessagePublishArgs<T>> handler)
        {
            if (handler == null) throw new ArgumentNullException(nameof(handler));

            if (_postHandlers == null)
            {
                _postHandlers = new List<Action<MessagePublishArgs<T>>>();
            }

            _postHandlers.Add(handler);
        }

        Task IMessagePublisher<T>.Publish(T message)
        {
            return Publish(message);
        }
    }
}