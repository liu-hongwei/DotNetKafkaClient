using System.IO;
using System.Text;

namespace DotNetKafkaClient
{
    public abstract class KafkaRequest : KafkaPacket
    {
        
        public abstract ApiKey ApiKey { get; }
        public abstract short  ApiVersion { get; set; }
        public abstract string ClientId { get; set; }

        public abstract byte[] Serialize();

        public const string DefaultEncoding = "UTF-8";
        public const short DefaultTopicLengthIfNonePresent = 2;

        protected const byte DefaultRequestSizeSize = 4;
        protected const byte DefaultRequestIdSize = 2;
        protected short GetShortStringLength(string text, string encoding = DefaultEncoding)
        {
            if (string.IsNullOrEmpty(text))
            {
                return (short)0;
            }
            else
            {
                Encoding encoder = Encoding.GetEncoding(encoding);
                return (short)encoder.GetByteCount(text);
            }
        }

        public short GetShortStringWriteLength(string text, string encoding = DefaultEncoding)
        {
            return (short)(2 + GetShortStringLength(text, encoding));
        }
    }
}