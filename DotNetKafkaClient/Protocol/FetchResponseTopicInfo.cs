using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DotNetKafkaClient
{
    public class FetchResponseTopicInfo
    {
        public const byte DefaultNumberOfPartitionsSize = 4;
        public const byte DefaultTopicSizeSize = 2;

        public FetchResponseTopicInfo(string topic, IEnumerable<FetchResponsePartitionInfo> partitionData)
        {
            this.Topic = topic;
            this.PartitionData = partitionData;
        }

        public string Topic { get; private set; }

        public IEnumerable<FetchResponsePartitionInfo> PartitionData { get; private set; }

        //public int SizeInBytes
        //{
        //    get
        //    {
        //        var topicLength = GetTopicLength(this.Topic);
        //        return DefaultTopicSizeSize + topicLength + DefaultNumberOfPartitionsSize + this.PartitionData.Sum(dataPiece => dataPiece.SizeInBytes);
        //    }
        //}


        protected static short GetTopicLength(string topic, string encoding = KafkaRequest.DefaultEncoding)
        {
            Encoding encoder = Encoding.GetEncoding(encoding);
            return string.IsNullOrEmpty(topic) ? KafkaRequest.DefaultTopicLengthIfNonePresent : (short)encoder.GetByteCount(topic);
        }
    }
}