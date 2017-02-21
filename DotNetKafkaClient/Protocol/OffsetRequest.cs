using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DotNetKafkaClient
{
    public class OffsetRequest : KafkaRequest
    {
        /// <summary>
        /// The latest time constant.
        /// </summary>
        public static readonly long LatestTime = -1L;

        /// <summary>
        /// The earliest time constant.
        /// </summary>
        public static readonly long EarliestTime = -2L;

        public const string SmallestTime = "smallest";
        public const string LargestTime = "largest";
        public const byte DefaultTopicSizeSize = 2;
        public const byte DefaultPartitionSize = 4;
        public const byte DefaultTimeSize = 8;
        public const byte DefaultReplicaIdSize = 4;
        public const byte DefaultRequestInfoSize = 4;
        public const byte DefaultPartitionCountSize = 4;
        public const byte DefaultHeaderSize = DefaultRequestSizeSize + DefaultRequestIdSize +
                                              2 + // version
                                              4;  // correlation            

        public override int Size
        {
            get
            {
                return
                    DefaultHeaderSize +
                    GetShortStringWriteLength(ClientId) +
                    DefaultReplicaIdSize +
                    DefaultRequestInfoSize +
                    RequestInfo.Keys.Sum(k => GetShortStringWriteLength(k)) +
                    RequestInfo.Values.Sum(
                        v => DefaultPartitionCountSize + v.Count * PartitionOffsetRequestInfo.SizeInBytes);
            }
            set { }
        }

        /// <summary>
        /// Initializes a new instance of the OffsetRequest class.
        /// </summary>        
        public OffsetRequest(Dictionary<string, List<PartitionOffsetRequestInfo>> requestInfo,
            short apiVersion = 0,
            int correlationId = 0,
            string clientId = "",
            int replicaId = -1)
        {
            ApiVersion = apiVersion;
            ClientId = clientId;
            CorrelationId = correlationId;
            ReplicaId = replicaId;
            RequestInfo = requestInfo;
        }

        public override short ApiVersion { get; set; }
        public int ReplicaId { get; set; }
        public override int CorrelationId { get; set; }
        public override string ClientId { get; set; }
        public Dictionary<string, List<PartitionOffsetRequestInfo>> RequestInfo { get; private set; }
        public override ApiKey ApiKey
        {
            get
            {
                return ApiKey.Offsets;
            }
        }

        public override byte[] Serialize()
        {
            using (var ms = new MemoryStream(this.Size))
            using (var writer = new KafkaBinaryWriter(ms))
            {
                writer.Write(ms.Capacity - DefaultRequestSizeSize);
                writer.Write((short)this.ApiKey);
                writer.Write(ApiVersion);
                writer.Write(CorrelationId);
                writer.WriteShortString(ClientId);
                writer.Write(ReplicaId);
                writer.Write(RequestInfo.Count);
                foreach (var kv in RequestInfo)
                {
                    writer.WriteShortString(kv.Key);
                    writer.Write(kv.Value.Count);
                    foreach (var info in kv.Value)
                    {
                        info.WriteTo(writer);
                    }
                }

                return ms.GetBuffer();
            }
        }
        

    }
}