using System.IO;

namespace DotNetKafkaClient
{
    public class HeartbeatRequest : KafkaRequest
    {
        public override short ApiVersion { get; set; }
        public override int CorrelationId { get; set; }
        public override string ClientId { get; set; }

        /*
         HeartbeatRequest => GroupId GenerationId MemberId
          GroupId => string
          GenerationId => int32
          MemberId => string
         */

        public string GroupId { get; set; }
        public int GenerationId { get; set; }
        public string MemberId { get; set; }


        public HeartbeatRequest(string groupId, int generationid, string memberId, short apiVersion = 0, int correlationId = 0, string clientId = "")
        {
            this.ApiVersion = apiVersion;
            this.CorrelationId = correlationId;
            this.ClientId = clientId;

            this.GroupId = groupId;
            this.GenerationId = generationid;
            this.MemberId = memberId;
        }

        public override ApiKey ApiKey { get { return ApiKey.Heartbeat; } }

        public override byte[] Serialize()
        {
            using (var ms = new MemoryStream(this.Size))
            {
                using (var writer = new KafkaBinaryWriter(ms))
                {
                    writer.Write(ms.Capacity - 4);
                    writer.Write((short)this.ApiKey);
                    writer.Write(ApiVersion);
                    writer.Write(CorrelationId);
                    writer.WriteShortString(ClientId);

                    writer.WriteShortString(this.GroupId);
                    writer.Write(this.GenerationId);
                    writer.WriteShortString(this.MemberId);

                    return ms.GetBuffer();
                }
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
                sizeOfBody += 4; // generation id
                sizeOfBody += GetShortStringWriteLength(this.MemberId);
                return sizeOfHeader + sizeOfBody;

            }
            set { }
        }
    }
}