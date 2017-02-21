using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DotNetKafkaClient
{
    /// <summary>
    /// v2 (supported in 0.9.0 or later)
    /// </summary>
    public class OffsetCommitRequest : KafkaRequest
    {
        public override short ApiVersion { get;  set; }
        public override int CorrelationId { get;  set; }
        public override string ClientId { get;  set; }

        public string ConsumerGroupId { get; set; }
        public int ConsumerGroupGenerationId { get; set; }

        public string ConsumerId { get; set; }

        public long RetentionTime { get; set; }

        public Dictionary<string, List<PartitionOffsetCommitRequestInfo>> RequestInfo { get; private set; }

        public OffsetCommitRequest(string consumerGroupId, int groupGeneratioId, string consumerId, long retentionTime, Dictionary<string, List<PartitionOffsetCommitRequestInfo>> requestInfo, short apiVersion = 2, int correlationId = 0, string clientId = "")
        {
            /*
             In v2, we removed the time stamp field but add a global retention time field (see KAFKA-1634 for details); 
             brokers will then always set the commit time stamp as the receive time, but the committed offset can be retained until 
             its commit time stamp + user specified retention time in the commit request. If the retention time is not set (-1), the broker offset retention time will be used as default.
             Note that when this API is used for a "simple consumer," which is not part of a consumer group, then the generationId must be set to -1 and the memberId must be empty (not null). 
             Additio nally, if there is an active consumer group with the same groupId, then the commit will be rejected (typically with an UNKNOWN_MEMBER_ID or ILLEGAL_GENERATION error). 
             */

            this.ConsumerGroupId = consumerGroupId;
            this.ConsumerGroupGenerationId = groupGeneratioId;
            this.ConsumerId = consumerId;
            this.RetentionTime = retentionTime; // if retention time is -1, kafka will use default offset retention , which is 24*60*60*1000 (found from source code)
            this.RequestInfo = requestInfo;

            // for kafka 0.9.0 or later
            // version 0 will commits to zookeeper, version 1 and above will commit to kafka
            this.ApiVersion = apiVersion;
            this.CorrelationId = correlationId;
            this.ClientId = clientId;
        }

        public override ApiKey ApiKey { get { return ApiKey.OffsetCommit; } }
        public override byte[] Serialize()
        {
            using (var ms = new MemoryStream(this.Size))
            {
                using (var writer = new KafkaBinaryWriter(ms))
            {
                writer.Write(ms.Capacity - 4);
                writer.Write((short)this.ApiKey);
                writer.Write(ApiVersion);
                writer.Write(CorrelationId);
                writer.WriteShortString(ClientId);
                writer.WriteShortString(ConsumerGroupId);
                writer.Write(ConsumerGroupGenerationId);
                writer.WriteShortString(ConsumerId);
                writer.Write(RetentionTime);
                writer.Write(RequestInfo.Count);
                foreach (var kv in RequestInfo)
                {
                    writer.WriteShortString(kv.Key);
                    writer.Write(kv.Value.Count);
                    foreach (var info in kv.Value)
                    {
                        writer.Write(info.PartitionId);
                        writer.Write(info.Offset);
                        writer.WriteShortString(info.Metadata);
                    }
                }

                return ms.GetBuffer();
            }}
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
                sizeOfBody += GetShortStringWriteLength(this.ConsumerGroupId);
                sizeOfBody += 4;
                sizeOfBody += GetShortStringWriteLength(this.ConsumerId);
                sizeOfBody += 8;

                sizeOfBody += 4;
                sizeOfBody += this.RequestInfo.Keys.Sum(k => GetShortStringWriteLength(k));
                sizeOfBody += this.RequestInfo.Values.Sum(v => 4 + v.Sum(e => 4 + 8 + GetShortStringWriteLength(e.Metadata)));

                return sizeOfHeader + sizeOfBody;
            }
            set { }
        }
    }
}