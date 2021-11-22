using System.Threading.Tasks;

namespace NimbleRabbit
{
    public interface IMessagePublisher<in T>
    {
        Task Publish(T message);
    }
}