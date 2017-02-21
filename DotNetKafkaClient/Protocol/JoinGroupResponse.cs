using System.Collections.Generic;
using System.Linq;

namespace DotNetKafkaClient
{
    public class JoinGroupResponse : KafkaResponse
    {
        public override int Size { get; set; }
        public override int CorrelationId { get; set; }


        /*
         v0 and v1 supported in 0.9.0 and greater
        JoinGroupResponse => ErrorCode GenerationId GroupProtocol LeaderId MemberId Members
          ErrorCode => int16
          GenerationId => int32
          GroupProtocol => string
          LeaderId => string
          MemberId => string
          Members => [MemberId MemberMetadata]
            MemberId => string
            MemberMetadata => bytes
         */

        public short ErrorCode { get; set; }
        public int GenerationId { get; set; }
        public string GroupProtocol { get; set; }
        public string LeaderId { get; set; }
        public string MemberId { get; set; }
        public List<JoinGroupResponseMemberInfo> Members { get; set; }

        public JoinGroupResponse(short errorCode, int generationId, string groupProtocol, string leaderId,
            string memberId, IEnumerable<JoinGroupResponseMemberInfo> memberInfos)
        {
            this.ErrorCode = errorCode;
            this.GenerationId = generationId;
            this.GroupProtocol = groupProtocol;
            this.LeaderId = leaderId;
            this.MemberId = memberId;
            this.Members = memberInfos.ToList();
        }

        public Dictionary<string, JoinGroupResponseMemberMetadataInfo> GetMembers()
        {
            var members = new Dictionary<string, JoinGroupResponseMemberMetadataInfo>();
            foreach (var member in this.Members)
            {
                if (!members.ContainsKey(member.MemberId))
                {
                    members.Add(member.MemberId, member.ParseMemberMetadata());
                }
                else
                {
                    members[member.MemberId] = member.ParseMemberMetadata();
                }
            }

            return members;
        }

        public static JoinGroupResponse ParseFrom(KafkaBinaryReader reader)
        {

            var size = reader.ReadInt32();
            var correlationid = reader.ReadInt32();

            var error = reader.ReadInt16();
            var generationid = reader.ReadInt32();
            var groupprotocol = reader.ReadShortString();
            var leaderid = reader.ReadShortString();
            var memberid = reader.ReadShortString();

            var count = reader.ReadInt32();
            var members = new JoinGroupResponseMemberInfo[count];
            for (int i = 0; i < count; i++)
            {
                var id = reader.ReadShortString();
                var bytes = reader.ReadInt32();
                var metadata = reader.ReadBytes(bytes);
                members[i] = new JoinGroupResponseMemberInfo(id, metadata);
            }

            return new JoinGroupResponse(error, generationid, groupprotocol, leaderid, memberid, members);
        }

       
    }
}