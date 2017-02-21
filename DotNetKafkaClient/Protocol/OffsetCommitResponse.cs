using System.Collections.Generic;

namespace DotNetKafkaClient
{
    public class OffsetCommitResponse : KafkaResponse
    {
        public override int Size { get;  set; }
        public override int CorrelationId { get;  set; }

        public Dictionary<string, List<PartitionOffsetCommitResponseInfo>> ResponseInfo { get; private set; }

        public OffsetCommitResponse(int size, int correlationId, Dictionary<string, List<PartitionOffsetCommitResponseInfo>> responseInfo)
        {
            this.Size = size;
            this.CorrelationId = correlationId;
            this.ResponseInfo = responseInfo;
        }

        public static OffsetCommitResponse ParseFrom(KafkaBinaryReader reader)
        {
            var size = reader.ReadInt32();
            var correlationid = reader.ReadInt32();
            var count = reader.ReadInt32();
            var data = new Dictionary<string, List<PartitionOffsetCommitResponseInfo>>();
            for (int i = 0; i < count; i++)
            {
                var topic = reader.ReadShortString();
                var num = reader.ReadInt32();
                var info = new List<PartitionOffsetCommitResponseInfo>();
                for (int j = 0; j < num; j++)
                {
                    var partition = reader.ReadInt32();
                    var errorCode = reader.ReadInt16();
                    info.Add(new PartitionOffsetCommitResponseInfo(partition, errorCode));
                }

                if (!data.ContainsKey(topic))
                {
                    data.Add(topic, info);
                }
                else
                {
                    data[topic] = info;
                }
            }

            var response = new OffsetCommitResponse(size, correlationid, data);
            return response;
        }

        
    }
}