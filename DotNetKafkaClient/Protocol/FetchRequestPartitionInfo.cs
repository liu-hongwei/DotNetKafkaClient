using System.IO;

namespace DotNetKafkaClient
{
    public class FetchRequestPartitionInfo
    {
        public int PartitionId { get; set; }
        public long Offset { get; set; }
        public int FetchSize { get; set; }

        public FetchRequestPartitionInfo(int partitionId, long offset, int fetchSize)
        {
            PartitionId = partitionId;
            Offset = offset;
            FetchSize = fetchSize;
        }

        public int SizeInBytes
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
            writer.Write(Offset);
            writer.Write(FetchSize);
        }

        public override string ToString()
        {
            return string.Format("PartitionId:{0},Offset:{1},FetchSize:{2}", PartitionId, Offset, FetchSize);
        }
    }
}