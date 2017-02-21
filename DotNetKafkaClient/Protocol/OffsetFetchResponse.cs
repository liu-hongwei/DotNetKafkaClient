using System.Collections.Generic;

namespace DotNetKafkaClient
{
    public class OffsetFetchResponse : KafkaResponse
    {
        public override int Size { get;  set; }
        public override int CorrelationId { get;  set; }

        /////////////////////////////////////////////////////
        /*
v0 and v1 (supported in 0.8.2 or after):
OffsetFetchResponse => [TopicName [Partition Offset Metadata ErrorCode]]
  TopicName => string
  Partition => int32
  Offset => int64
  Metadata => string
  ErrorCode => int16
  */

        public Dictionary<string, List<OffsetFetchResponseInfo>> ResponseInfo { get; set; }

        public OffsetFetchResponse(int size, int correlationId, Dictionary<string, List<OffsetFetchResponseInfo>> responseInfo)
        {
            this.Size = size;
            this.CorrelationId = correlationId;

            this.ResponseInfo = responseInfo;
        }

        public static OffsetFetchResponse ParseFrom(KafkaBinaryReader reader)
        {
            var size = reader.ReadInt32();
            var correlationid = reader.ReadInt32();
            var count = reader.ReadInt32();

            var data = new Dictionary<string, List<OffsetFetchResponseInfo>>();
            for (int i = 0; i < count; i++)
            {
                var topic = reader.ReadShortString();

                var num = reader.ReadInt32();
                for (int j = 0; j < num; j++)
                {
                    var partition = reader.ReadInt32();
                    var offset = reader.ReadInt64();
                    var metadata = reader.ReadShortString();
                    var error = reader.ReadInt16();

                    if (!data.ContainsKey(topic))
                    {
                        data.Add(topic, new List<OffsetFetchResponseInfo>());
                    }

                    data[topic].Add(new OffsetFetchResponseInfo(partition, offset, metadata, error));
                }
            }

            return new OffsetFetchResponse(size, correlationid, data);
        }

      
    }
}