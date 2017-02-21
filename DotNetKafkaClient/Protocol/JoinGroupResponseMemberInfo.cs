namespace DotNetKafkaClient
{
    public class JoinGroupResponseMemberInfo
    {
        public string MemberId { get; set; }
        public byte[] MemberMetadata { get; set; }

        public JoinGroupResponseMemberInfo(string memberId, byte[] memberMetadata)
        {
            this.MemberId = memberId;
            this.MemberMetadata = memberMetadata;
        }

        public JoinGroupResponseMemberMetadataInfo ParseMemberMetadata()
        {
            var metadata = new JoinGroupResponseMemberMetadataInfo();
            metadata.Deserialize(this.MemberMetadata);
            return metadata;
        }
    }
}