using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace DotNetKafkaClient
{
    public class PartitionOffsetsResponse : KafkaResponse
    {
        public override int Size { get; set; }
        public override int CorrelationId { get; set; }

        public int PartitionId { get; private set; }
        public KafkaErrorCodes Error { get; private set; }
        public List<long> Offsets { get; private set; }

        public PartitionOffsetsResponse(int partitionId, short error, List<long> offsets)
        {
            PartitionId = partitionId;
            Error = (KafkaErrorCodes)error;
            Offsets = offsets;
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder(1024);

            sb.AppendFormat("PartitionOffsetsResponse.PartitionId:{0},Error:{1},Offsets Count={2}", this.PartitionId, this.Error, this.Offsets.Count);
            int i = 0;
            foreach (var o in this.Offsets)
            {
                sb.AppendFormat("Offsets[{0}]:{1}", i, o.ToString());
                i++;
            }

            string s = sb.ToString();
            sb.Length = 0;
            return s;
        }

        public static PartitionOffsetsResponse ReadFrom(KafkaBinaryReader reader)
        {
            var partitionId = reader.ReadInt32();
            var error = reader.ReadInt16();
            var numOffsets = reader.ReadInt32();
            var offsets = new List<long>();
            for (int o = 0; o < numOffsets; ++o)
            {
                offsets.Add(reader.ReadInt64());
            }

            return new PartitionOffsetsResponse(partitionId,error,offsets);
        }
    }
}