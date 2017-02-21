namespace DotNetKafkaClient
{
    public class SyncGroupResponsePartitionAssignmentInfo
    {
        public string Topic { get; set; }
        public int[] Partitions { get; set; }
    }
}