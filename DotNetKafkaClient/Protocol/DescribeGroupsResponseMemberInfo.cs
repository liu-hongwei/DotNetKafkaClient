namespace DotNetKafkaClient
{
    public class DescribeGroupsResponseMemberInfo
    {
        public string MemberId { get; set; }
        public string ClientId { get; set; }
        public string ClientHost { get; set; }
        public byte[] MemberMetadata { get; set; }
        public byte[] MemberAssignment { get; set; }

        public DescribeGroupsResponseMemberInfo(string memberId, string clientId, string clientHost, byte[] metadata,
            byte[] assignment)
        {
            this.MemberId = memberId;
            this.ClientId = clientId;
            this.ClientHost = clientHost;
            this.MemberMetadata = metadata;
            this.MemberAssignment = assignment;
        }

        public object ParseMemberMetadata()
        {
            return null;
        }

        public object ParseMemberAssignment()
        {
            return null;
        }

    }
}