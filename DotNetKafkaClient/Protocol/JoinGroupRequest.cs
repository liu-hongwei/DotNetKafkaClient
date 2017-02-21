using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DotNetKafkaClient
{
    public class JoinGroupRequest : KafkaRequest
    {
        public override short ApiVersion { get; set; }
        public override int CorrelationId { get; set; }
        public override string ClientId { get; set; }


        /*
        v0 supported in 0.9.0.0 and greater
        JoinGroupRequest => GroupId SessionTimeout MemberId ProtocolType GroupProtocols
          GroupId => string
          SessionTimeout => int32
          MemberId => string
          ProtocolType => string
          GroupProtocols => [ProtocolName ProtocolMetadata]
            ProtocolName => string
            ProtocolMetadata => bytes


        v1 supported in 0.10.1.0 and greater
        JoinGroupRequest => GroupId SessionTimeout RebalanceTimeout MemberId ProtocolType GroupProtocols
          GroupId => string
          SessionTimeout => int32
          RebalanceTimeout => int32
          MemberId => string
          ProtocolType => string
          GroupProtocols => [ProtocolName ProtocolMetadata]
            ProtocolName => string
            ProtocolMetadata => bytes
         */

        public string GroupId { get; set; }
        public int SessionTimeout { get; set; }

        /*
        // in version 1
        public int ReblanceTimeout { get; set; }
        */

        public string MemberId { get; set; }

        public string ProtocolType { get; set; }

        public List<JoinGroupRequestProtocolInfo> GroupProtocols { get; set; }

        public JoinGroupRequest(string clientId, string groupId, int sessionTimeout, string memberId, string protocolType, IEnumerable<JoinGroupRequestProtocolInfo> groupProtocols, short apiVersion = 0, int correlationId = 0)
        {
            // TODO : how to dynamically figure out the version ?
            // version 0 only supported in 0.9.0.0 and greater, 
            // version 1 only supported in 0.10.1.0 and greater
            this.ApiVersion = apiVersion;
            this.CorrelationId = correlationId;
            this.ClientId = clientId;

            this.GroupId = groupId;
            this.SessionTimeout = sessionTimeout;
            this.MemberId = memberId;
            this.ProtocolType = protocolType;
            this.GroupProtocols = groupProtocols.ToList();
        }

        public override ApiKey ApiKey
        {
            get
            {
                return ApiKey.JoinGroup;
            }
        }
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
                    writer.Write(this.SessionTimeout);

                    /*
                    // in version 1
                    writer.Write(this.ReblanceTimeout);
                    */

                    writer.WriteShortString(this.MemberId);
                    writer.WriteShortString(this.ProtocolType);

                    writer.Write(this.GroupProtocols.Count);
                    for (int i = 0; i < this.GroupProtocols.Count; i++)
                    {
                        writer.WriteShortString(this.GroupProtocols[i].ProtocolName);

                        writer.Write(this.GroupProtocols[i].ProtocolMetadata.Length);
                        writer.Write(this.GroupProtocols[i].ProtocolMetadata);
                    }

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

                sizeOfBody += GetShortStringWriteLength(this.GroupId); // groupid
                sizeOfBody += 4; // sessiontimeout

                /*
                // in version 1
                sizeOfBody += 4; // reblancetimeout
                */

                sizeOfBody += GetShortStringWriteLength(this.MemberId); // memberid
                sizeOfBody += GetShortStringWriteLength(this.ProtocolType);

                sizeOfBody += 4; // group protocols size
                for (int i = 0; i < this.GroupProtocols.Count; i++)
                {
                    sizeOfBody += GetShortStringWriteLength(this.GroupProtocols[i].ProtocolName);

                    sizeOfBody += 4; // size of metadata
                    sizeOfBody += this.GroupProtocols[i].ProtocolMetadata.Length;
                }

                return sizeOfHeader + sizeOfBody;
            }
            set { }
        }
    }
}