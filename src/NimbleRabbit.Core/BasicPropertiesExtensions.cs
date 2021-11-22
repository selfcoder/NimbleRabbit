using System;
using RabbitMQ.Client;

namespace NimbleRabbit
{
    public static class BasicPropertiesExtensions
    {
        public static void CopyTo(this IBasicProperties props, IBasicProperties dest)
        {
            if (props == null) throw new ArgumentNullException(nameof(props));
            if (dest == null) throw new ArgumentNullException(nameof(dest));

            if (props.IsAppIdPresent()) dest.AppId = props.AppId;
            if (props.IsClusterIdPresent()) dest.ClusterId = props.ClusterId;
            if (props.IsContentEncodingPresent()) dest.ContentEncoding = props.ContentEncoding;
            if (props.IsContentTypePresent()) dest.ContentType = props.ContentType;
            if (props.IsCorrelationIdPresent()) dest.CorrelationId = props.CorrelationId;
            if (props.IsDeliveryModePresent()) dest.DeliveryMode = props.DeliveryMode;
            if (props.IsExpirationPresent()) dest.Expiration = props.Expiration;
            if (props.IsHeadersPresent()) dest.Headers = props.Headers;
            if (props.IsMessageIdPresent()) dest.MessageId = props.MessageId;
            if (props.IsPriorityPresent()) dest.Priority = props.Priority;
            if (props.IsReplyToPresent()) dest.ReplyTo = props.ReplyTo;
            if (props.IsTimestampPresent()) dest.Timestamp = props.Timestamp;
            if (props.IsTypePresent()) dest.Type = props.Type;
            if (props.IsUserIdPresent()) dest.UserId = props.UserId;
        }

        public static string GetMessageIdIfExists(this IBasicProperties props)
        {
            return props.IsMessageIdPresent() ? props.MessageId : null;
        }
    }
}