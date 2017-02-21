using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DotNetKafkaClient
{
    public class SyncGroupRequest : KafkaRequest
    {
        public override short ApiVersion { get; set; }
        public override int CorrelationId { get; set; }
        public override string ClientId { get; set; }

        /*
         SyncGroupRequest => GroupId GenerationId MemberId GroupAssignment
          GroupId => string
          GenerationId => int32
          MemberId => string
          GroupAssignment => [MemberId MemberAssignment]
            MemberId => string
            MemberAssignment => bytes
         */

        public string GroupId { get; set; }
        public int GenerationId { get; set; }
        public string MemberId { get; set; }
        public List<SyncGroupRequestGroupAssignmentInfo> GroupAssignmentInfos { get; set; }

        public SyncGroupRequest(string groupId, int generationId, string memberId,
            IEnumerable<SyncGroupRequestGroupAssignmentInfo> groupAssignmentInfos, short apiVersion = 0, int correlationId = 0, string clientId = "")
        {
            this.ApiVersion = apiVersion;
            this.CorrelationId = correlationId;
            this.ClientId = clientId;

            this.GroupId = groupId;
            this.GenerationId = generationId;
            this.MemberId = memberId;
            this.GroupAssignmentInfos = groupAssignmentInfos.ToList();

        }

        public override ApiKey ApiKey
        {
            get
            {
                return ApiKey.SyncGroup;
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
                    writer.Write(this.GenerationId);
                    writer.WriteShortString(this.MemberId);

                    writer.Write(this.GroupAssignmentInfos.Count);
                    for (int i = 0; i < this.GroupAssignmentInfos.Count; i++)
                    {
                        writer.WriteShortString(this.GroupAssignmentInfos[i].MemberId);

                        if (this.GroupAssignmentInfos[i].MemberAssignment != null)
                        {
                            writer.Write(this.GroupAssignmentInfos[i].MemberAssignment.Length);
                            writer.Write(this.GroupAssignmentInfos[i].MemberAssignment);
                        }
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

                sizeOfBody += GetShortStringWriteLength(this.GroupId);
                sizeOfBody += 4; // generationid
                sizeOfBody += GetShortStringWriteLength(this.MemberId);

                sizeOfBody += 4;
                for (int i = 0; i < this.GroupAssignmentInfos.Count; i++)
                {
                    sizeOfBody += GetShortStringWriteLength(this.GroupAssignmentInfos[i].MemberId);

                    sizeOfBody += 4;
                    if (this.GroupAssignmentInfos[i].MemberAssignment != null)
                    {
                        sizeOfBody += this.GroupAssignmentInfos[i].MemberAssignment.Length;
                    }
                }

                return sizeOfHeader + sizeOfBody;
            }
            set { }
        }
    }
}