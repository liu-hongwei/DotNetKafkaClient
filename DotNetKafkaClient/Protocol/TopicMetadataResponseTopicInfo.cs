using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DotNetKafkaClient
{
    public class TopicMetadataResponseTopicInfo
    {
        public const byte DefaultNumOfPartitionsSize = 4;

        public TopicMetadataResponseTopicInfo(string topic, IEnumerable<TopicMetadataResponsePartitionInfo> partitionsMetadata, short error)
        {
            this.Topic = topic;
            this.PartitionsMetadata = partitionsMetadata;
            Error = (KafkaErrorCodes)error;
        }


        public string Topic { get; private set; }
        public IEnumerable<TopicMetadataResponsePartitionInfo> PartitionsMetadata { get; private set; }
        public KafkaErrorCodes Error { get; private set; }
        public int SizeInBytes
        {
            get
            {
                var size = (int)KafkaPrimitiveTypes.GetShortStringLength(this.Topic, KafkaRequest.DefaultEncoding);
                foreach (var partitionMetadata in this.PartitionsMetadata)
                {
                    size += DefaultNumOfPartitionsSize + partitionMetadata.SizeInBytes;
                }
                return size;
            }
        }

        public void WriteTo(System.IO.MemoryStream output)
        {
            //Guard.NotNull(output, "output");

            using (var writer = new KafkaBinaryWriter(output))
            {
                this.WriteTo(writer);
            }
        }

        public void WriteTo(KafkaBinaryWriter writer)
        {
            //Guard.NotNull(writer, "writer");

            writer.WriteShortString(this.Topic, KafkaRequest.DefaultEncoding);
            writer.Write(this.PartitionsMetadata.Count());
            foreach (var partitionMetadata in PartitionsMetadata)
            {
                partitionMetadata.WriteTo(writer);
            }
        }

        internal static TopicMetadataResponseTopicInfo ParseFrom(KafkaBinaryReader reader, Dictionary<int, Broker> brokers)
        {
            var errorCode = reader.ReadInt16();
            var topic = KafkaPrimitiveTypes.ReadShortString(reader, KafkaRequest.DefaultEncoding);
            var numPartitions = reader.ReadInt32();
            var partitionsMetadata = new List<TopicMetadataResponsePartitionInfo>();
            for (int i = 0; i < numPartitions; i++)
            {
                partitionsMetadata.Add(TopicMetadataResponsePartitionInfo.ParseFrom(reader, brokers));
            }
            return new TopicMetadataResponseTopicInfo(topic, partitionsMetadata, errorCode);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder(1024);
            sb.AppendFormat("TopicMetaData.Topic:{0},Error:{1},PartitionMetaData Count={2}", this.Topic, this.Error.ToString(), this.PartitionsMetadata.Count());
            sb.AppendLine();

            int j = 0;
            foreach (var p in this.PartitionsMetadata)
            {
                sb.AppendFormat("PartitionMetaData[{0}]:{1}", j, p.ToString());
                j++;
            }

            string s = sb.ToString();
            sb.Length = 0;
            return s;
        }
    }
}