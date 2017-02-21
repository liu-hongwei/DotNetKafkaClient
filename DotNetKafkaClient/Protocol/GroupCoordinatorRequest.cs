using System.IO;

namespace DotNetKafkaClient
{
    public class GroupCoordinatorRequest : KafkaRequest
    {
        public override short ApiVersion { get;  set; }
        public override int CorrelationId { get; set; }
        public override string ClientId { get;  set; }

        /*
            GroupCoordinatorRequest => GroupId
              GroupId => string
        */


        public string GroupId;

        public GroupCoordinatorRequest(string groupId, short version = 0, int correlation = 0, string client = "")
        {
            this.ApiVersion = version;
            this.CorrelationId = correlation;
            this.ClientId = client;

            this.GroupId = groupId;
        }

        public override ApiKey ApiKey
        {
            get
            {
                return ApiKey.GroupCoordinator;
            }
        }

        public override byte[] Serialize()
        {
            using (var ms = new MemoryStream(this.Size))
            using (var writer = new KafkaBinaryWriter(ms))
            {
                writer.Write(ms.Capacity - 4);
                writer.Write((short)this.ApiKey);
                writer.Write(ApiVersion);
                writer.Write(CorrelationId);
                writer.WriteShortString(ClientId);

                writer.WriteShortString(this.GroupId);

                return ms.GetBuffer();
            }
        }
        
        public override int Size
        {
            get
            {
                // length in bytes
                //-----------------------------------------------------------------------\\
                // +-------------+
                // | size        |  4bytes
                // +-------------+
                // | api key     |  2bytes
                // +-------------+
                // | api version |  2bytes
                // +-------------+
                // |correlationid|  4bytes      
                // +-------------+                   +------+-----------+
                // | clientId    |  string ->        | len  | ....      |
                // +-------------+                   +------+-----------+
                // |  body       |                    2bytes   {len}bytes
                // +-------------+
                //-----------------------------------------------------------------------\\
                var sizeOfHeader = 4 + 2 + 2 + 4 + GetShortStringWriteLength(ClientId);
                var sizeOfBody = 0;
                sizeOfBody += GetShortStringWriteLength(this.GroupId);
                return sizeOfHeader + sizeOfBody;

            }
            set { }
        }
    }
}