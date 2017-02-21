using System.Collections.Generic;
using System.Text;

namespace DotNetKafkaClient
{
    public class OffsetResponse : KafkaResponse
    {
        public override int Size { get; set; }
        public override int CorrelationId { get;  set; }
        public Dictionary<string, List<PartitionOffsetsResponse>> ResponseMap { get; private set; }

        public OffsetResponse(int correlationId, Dictionary<string, List<PartitionOffsetsResponse>> responseMap)
        {
            CorrelationId = correlationId;
            ResponseMap = responseMap;
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder(1024);
            sb.AppendFormat("OffsetResponse.CorrelationId:{0},ResponseMap Count={1}", this.CorrelationId, this.ResponseMap.Count);

            int i = 0;
            foreach (var v in this.ResponseMap)
            {
                sb.AppendFormat(",ResponseMap[{0}].Key:{1},PartitionOffsetsResponse Count={2}", i, v.Key, v.Value.Count);
                int j = 0;
                foreach (var o in v.Value)
                {
                    sb.AppendFormat(",PartitionOffsetsResponse[{0}]:{1}", j, o.ToString());
                    j++;
                }
                i++;
            }

            string s = sb.ToString();
            sb.Length = 0;
            return s;
        }


        public static OffsetResponse ParseFrom(KafkaBinaryReader reader)
        {
            reader.ReadInt32(); // skipping first int
            var correlationId = reader.ReadInt32();
            var numTopics = reader.ReadInt32();
            var responseMap = new Dictionary<string, List<PartitionOffsetsResponse>>();
            for (int i = 0; i < numTopics; ++i)
            {
                var topic = reader.ReadShortString();
                var numPartitions = reader.ReadInt32();
                var responses = new List<PartitionOffsetsResponse>();
                for (int p = 0; p < numPartitions; ++p)
                {
                    responses.Add(PartitionOffsetsResponse.ReadFrom(reader));
                }

                responseMap[topic] = responses;
            }

            return new OffsetResponse(correlationId, responseMap);
        }

    }
}