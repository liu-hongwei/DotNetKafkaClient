using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DotNetKafkaClient
{
    public class OffsetFetchRequest : KafkaRequest
    {
        public override short ApiVersion { get; set; }
        public override int CorrelationId { get; set; }
        public override string ClientId { get; set; }

        //////////////////////////////////////////////
        /*
v0 and v1 (supported in 0.8.2 or after):
OffsetFetchRequest => ConsumerGroup [TopicName [Partition]]
  ConsumerGroup => string
  TopicName => string
  Partition => int32         */

        public string ConsumerGroup { get; set; }
        public Dictionary<string, List<int>> OffsetFetchRequestInfo { get; set; }

        public override ApiKey ApiKey
        {
            get { return ApiKey.OffsetFetch; }
        }

        public OffsetFetchRequest(string groupId, Dictionary<string, List<int>> requestInfo, short apiVersion = 1, int correlationid = 0, string clientId = "")
        {
            // version 0 will fetchs from zookeeper, version 1 and above will fetch from kafka
            this.ApiVersion = apiVersion;
            this.CorrelationId = correlationid;
            this.ClientId = clientId;

            this.ConsumerGroup = groupId;
            this.OffsetFetchRequestInfo = requestInfo;

        }

        public override byte[] Serialize()
        {
            using (var ms = new MemoryStream(this.Size))
            using (var writer = new KafkaBinaryWriter(ms))
            {
                writer.Write(ms.Capacity - 4);
                writer.Write((short)this.ApiKey);
                writer.Write(ApiVersion);
                writer.Write(CorrelationId);
                writer.WriteShortString(ClientId);

                writer.WriteShortString(this.ConsumerGroup);
                if (this.OffsetFetchRequestInfo != null)
                {
                    writer.Write(OffsetFetchRequestInfo.Count);
                    foreach (var info in this.OffsetFetchRequestInfo)
                    {
                        writer.WriteShortString(info.Key);
                        if (info.Value == null)
                        {
                            writer.Write(0);
                        }
                        else
                        {
                            writer.Write(info.Value.Count);
                            foreach (var p in info.Value)
                            {
                                writer.Write(p);
                            }
                        }
                    }
                }
                else
                {
                    writer.Write(0);
                }

                return ms.GetBuffer();
            }
        }
        
        public override int Size
        {
            get
            {
                // length in bytes
                //-----------------------------------------------------------------------\\
                // +-------------+
                // | size        |  4bytes
                // +-------------+
                // | api key     |  2bytes
                // +-------------+
                // | api version |  2bytes
                // +-------------+
                // |correlationid|  4bytes      
                // +-------------+                   +------+-----------+
                // | clientId    |  string ->        | len  | ....      |
                // +-------------+                   +------+-----------+
                // |  body       |                    2bytes   {len}bytes
                // +-------------+
                //-----------------------------------------------------------------------\\
                var sizeOfHeader = 4 + 2 + 2 + 4 + GetShortStringWriteLength(ClientId);
                var sizeOfBody = 0;
                sizeOfBody += GetShortStringWriteLength(this.ConsumerGroup);

                sizeOfBody += 4;

                if (this.OffsetFetchRequestInfo != null && this.OffsetFetchRequestInfo.Any())
                {
                    foreach (var kvp in this.OffsetFetchRequestInfo)
                    {
                        var topic = kvp.Key;
                        var partitions = kvp.Value;

                        sizeOfBody += GetShortStringWriteLength(topic);

                        sizeOfBody += 4;
                        if (partitions != null && partitions.Any())
                        {
                            sizeOfBody += partitions.Count * 4;
                        }
                    }
                }

                return sizeOfHeader + sizeOfBody;
            }
            set { }
        }
    }
}