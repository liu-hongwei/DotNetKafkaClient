using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace DotNetKafkaClient
{
    public class PartitionOffsetRequestInfo
    {
        public int PartitionId { get; private set; }
        public long Time { get; private set; }
        public int MaxNumOffsets { get; private set; }

        public PartitionOffsetRequestInfo(int partitionId, long time, int maxNumOffsets)
        {
            PartitionId = partitionId;
            Time = time;
            MaxNumOffsets = maxNumOffsets;
        }

        public static int SizeInBytes
        {
            get { return 4 + 8 + 4; }
        }

        public void WriteTo(MemoryStream output)
        {
            using (var writer = new KafkaBinaryWriter(output))
            {
                WriteTo(writer);
            }
        }

        public void WriteTo(KafkaBinaryWriter writer)
        {
            writer.Write(PartitionId);
            writer.Write(Time);
            writer.Write(MaxNumOffsets);
        }
    }

    /*
Offset Fetch Request (last commited offset)
Per the comment on  KAFKA-1841 - OffsetCommitRequest API - timestamp field is not versioned RESOLVED  , v0 and v1 are identical on the wire, but v0 (supported in 0.8.1 or later) reads offsets from zookeeper, while v1 (supported in 0.8.2 or later) reads offsets from kafka.
v0 and v1 (supported in 0.8.2 or after):
OffsetFetchRequest => ConsumerGroup [TopicName [Partition]]
ConsumerGroup => string
TopicName => string
Partition => int32
Offset Fetch Response
v0 and v1 (supported in 0.8.2 or after):
OffsetFetchResponse => [TopicName [Partition Offset Metadata ErrorCode]]
TopicName => string
Partition => int32
Offset => int64
Metadata => string
ErrorCode => int16
Note that if there is no offset associated with a topic-partition under that consumer group the broker does not set an error code (since it is not really an error), but returns empty metadata and sets the offset field to -1.
There is no format difference between Offset Fetch Request v0 and v1. Functionality wise, Offset Fetch Request v0 will fetch offset from zookeeper, Offset Fetch Request v1 will fetch offset from Kafka.
Possible Error Codes
* UNKNOWN_TOPIC_OR_PARTITION (3) <- only for request v0
* GROUP_LOAD_IN_PROGRESS (14)
* NOT_COORDINATOR_FOR_GROUP (16)
* ILLEGAL_GENERATION (22)
* UNKNOWN_MEMBER_ID (25)
* TOPIC_AUTHORIZATION_FAILED (29)
* GROUP_AUTHORIZATION_FAILED (30)    */
}