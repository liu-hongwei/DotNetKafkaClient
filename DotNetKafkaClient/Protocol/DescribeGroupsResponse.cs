using System.Collections.Generic;
using System.Linq;

namespace DotNetKafkaClient
{
    public class DescribeGroupsResponse : KafkaResponse
    {
        public override int Size { get; set; }
        public override int CorrelationId { get; set; }

        /*
DescribeGroupsResponse => [ErrorCode GroupId State ProtocolType Protocol Members]
  ErrorCode => int16
  GroupId => string
  State => string
  ProtocolType => string
  Protocol => string
  Members => [MemberId ClientId ClientHost MemberMetadata MemberAssignment]
    MemberId => string
    ClientId => string
    ClientHost => string
    MemberMetadata => bytes
    MemberAssignment => bytes
         */

        public List<DescribeGroupsResponseInfo> ResponseInfos { get; set; }

        public DescribeGroupsResponse(IEnumerable<DescribeGroupsResponseInfo> responseInfos)
        {
            this.ResponseInfos = responseInfos.ToList();
        }

        public static DescribeGroupsResponse ParseFrom(KafkaBinaryReader reader)
        {
            var size = reader.ReadInt32();
            var correlationid = reader.ReadInt32();

            var count = reader.ReadInt32();
            var responseInfos = new DescribeGroupsResponseInfo[count];
            for (int i = 0; i < count; i++)
            {
                var error = reader.ReadInt16();
                var groupid = reader.ReadShortString();
                var state = reader.ReadShortString();
                var protocolType = reader.ReadShortString();
                var protocol = reader.ReadShortString();
                var count2 = reader.ReadInt32();
                var members = new DescribeGroupsResponseMemberInfo[count2];
                for (int j = 0; j < count2; j++)
                {
                    var memberid = reader.ReadShortString();
                    var clientid = reader.ReadShortString();
                    var clienthost = reader.ReadShortString();

                    var metadataSize = reader.ReadInt32();
                    var metadata = reader.ReadBytes(metadataSize);
                    var assignmentSize = reader.ReadInt32();
                    var assignment = reader.ReadBytes(assignmentSize);

                    members[j] = new DescribeGroupsResponseMemberInfo(memberid, clientid, clienthost, metadata, assignment);
                }

                responseInfos[i] = new DescribeGroupsResponseInfo(error, groupid, state, protocolType, protocol, members);
            }

            return new DescribeGroupsResponse(responseInfos) { CorrelationId = correlationid, Size = size };
        }
        
    }
}