namespace DotNetKafkaClient
{
    public class JoinGroupRequestProtocolInfo
    {
        public string ProtocolName { get; set; }
        public byte[] ProtocolMetadata { get; set; }

        public JoinGroupRequestProtocolInfo(string protocolName, byte[] protocolMetadata)
        {
            this.ProtocolName = protocolName;
            this.ProtocolMetadata = protocolMetadata;
        }
    }
}