using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DotNetKafkaClient
{
    public class DescribeGroupsRequest : KafkaRequest
    {

        public override short ApiVersion { get; set; }
        public override int CorrelationId { get; set; }
        public override string ClientId { get; set; }

        /*
            DescribeGroupsRequest => [GroupId]
            GroupId => string
         */

        public List<string> GroupIds { get; set; }

        public DescribeGroupsRequest(IEnumerable<string> groupIds, short apiVersion = 0, int correlationId = 0, string clientId = "")
        {
            this.ApiVersion = apiVersion;
            this.CorrelationId = correlationId;
            this.ClientId = clientId;

            this.GroupIds = groupIds.ToList();
        }

        public override ApiKey ApiKey
        {
            get
            {
                return ApiKey.DescribeGroups;
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

                    writer.Write(this.GroupIds.Count);
                    foreach (var groupId in this.GroupIds)
                    {
                        writer.WriteShortString(groupId);
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

                sizeOfBody += 4;
                foreach (var groupId in this.GroupIds)
                {
                    sizeOfBody += GetShortStringWriteLength(groupId);
                }

                return sizeOfHeader + sizeOfBody;

            }
            set { }
        }
    }
}