namespace DotNetKafkaClient
{
    public class OffsetFetchResponseInfo
    {
        public int Partition { get; set; }
        public long Offset { get; set; }
        public string Metadata { get; set; }
        public int ErrorCode { get; set; }

        public OffsetFetchResponseInfo(int partition, long offset, string metadata, int errorcode)
        {
            this.Partition = partition;
            this.Offset = offset;
            this.Metadata = metadata;
            this.ErrorCode = errorcode;
        }
    }
}