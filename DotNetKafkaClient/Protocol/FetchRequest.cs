using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;

namespace DotNetKafkaClient
{
    public class FetchRequest : KafkaRequest
    {
        public const byte DefaultTopicSizeSize = 2;
        public const byte DefaultPartitionSize = 4;
        public const byte DefaultOffsetSize = 8;
        public const byte DefaultMaxSizeSize = 4;
        public const byte DefaultHeaderSize = DefaultRequestSizeSize + DefaultTopicSizeSize + DefaultPartitionSize + DefaultRequestIdSize + DefaultOffsetSize + DefaultMaxSizeSize;
        public const byte DefaultHeaderAsPartOfMultirequestSize = DefaultTopicSizeSize + DefaultPartitionSize + DefaultOffsetSize + DefaultMaxSizeSize;

        public const byte DefaultApiVersionSize = 2;
        public const byte DefaultCorrelationIdSize = 4;
        public const byte DefaultReplicaIdSize = 4;
        public const byte DefaultMaxWaitSize = 4;
        public const byte DefaultMinBytesSize = 4;
        public const byte DefaultOffsetInfoSizeSize = 4;

        public const short CurrentVersion = 0;
        public override short ApiVersion { get; set; }
        public override int CorrelationId { get; set; }
        public override string ClientId { get; set; }

        public FetchRequest(int correlationId, string clientId, int maxWait, int minBytes, Dictionary<string, List<FetchRequestPartitionInfo>> fetchInfos)
        {
            this.ApiVersion = CurrentVersion;
            this.CorrelationId = correlationId;
            this.ClientId = clientId;
            this.ReplicaId = -1;
            this.MaxWait = maxWait;
            this.MinBytes = minBytes;
            this.OffsetInfo = fetchInfos;
        }

        public override int Size
        {
            get
            {
                return DefaultRequestSizeSize +
                       DefaultRequestIdSize +
                       DefaultApiVersionSize +
                       DefaultCorrelationIdSize +
                       KafkaPrimitiveTypes.GetShortStringLength(this.ClientId, DefaultEncoding) +
                       DefaultReplicaIdSize +
                       DefaultMaxWaitSize +
                       DefaultMinBytesSize +
                       DefaultOffsetInfoSizeSize +
                       this.OffsetInfo.Keys.Sum(x => KafkaPrimitiveTypes.GetShortStringLength(x, DefaultEncoding)) +
                       this.OffsetInfo.Values.Select(pl => 4 + pl.Sum(p => p.SizeInBytes)).Sum();
            }
            set { }
        }

        public int ReplicaId { get; private set; }
        public int MaxWait { get; private set; }
        public int MinBytes { get; private set; }
        public Dictionary<string, List<FetchRequestPartitionInfo>> OffsetInfo { get; set; }

        public override ApiKey ApiKey
        {
            get
            {
                return ApiKey.Fetch;
            }
        }


        public override byte[] Serialize()
        {
            using (var ms = new MemoryStream(this.Size))
            {
                using (var writer = new KafkaBinaryWriter(ms))
                {
                    writer.Write(ms.Capacity - DefaultRequestSizeSize);
                    writer.Write((short)this.ApiKey);
                    writer.Write(this.ApiVersion);
                    writer.Write(this.CorrelationId);
                    writer.WriteShortString(this.ClientId);
                    writer.Write(this.ReplicaId);
                    writer.Write(this.MaxWait);
                    writer.Write(this.MinBytes);
                    writer.Write(this.OffsetInfo.Count);
                    foreach (var offsetInfo in this.OffsetInfo)
                    {
                        writer.WriteShortString(offsetInfo.Key);
                        writer.Write(offsetInfo.Value.Count);
                        foreach (var v in offsetInfo.Value)
                        {
                            v.WriteTo(writer);
                        }
                    }

                    return ms.GetBuffer();
                }
            }
        }
    }
}