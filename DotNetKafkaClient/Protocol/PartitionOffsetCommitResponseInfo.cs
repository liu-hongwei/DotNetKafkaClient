namespace DotNetKafkaClient
{
    public class PartitionOffsetCommitResponseInfo
    {
        public int PartitionId { get; set; }
        public short ErrorCode { get; set; }

        public PartitionOffsetCommitResponseInfo(int partitionId, short errorCode)
        {
            this.PartitionId = partitionId;
            this.ErrorCode = errorCode;
        }
    }
}