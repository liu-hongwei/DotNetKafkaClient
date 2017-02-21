namespace DotNetKafkaClient
{
    public class PartitionOffsetCommitRequestInfo
    {
        public int PartitionId { get; set; }
        public long Offset { get; set; }
        public string Metadata { get; set; }

        public PartitionOffsetCommitRequestInfo(int partitionId, long offset, string metadata)
        {
            this.PartitionId = partitionId;
            this.Offset = offset;
            this.Metadata = metadata;
        }
    }
}