using System.Text;

namespace DotNetKafkaClient
{
    public abstract class KafkaPacket
    {
        public abstract int Size { get; set; }
        public abstract int CorrelationId { get; set; }

    }
}