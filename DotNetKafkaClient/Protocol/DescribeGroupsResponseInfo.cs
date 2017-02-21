using System.Collections.Generic;
using System.Linq;

namespace DotNetKafkaClient
{
    public class DescribeGroupsResponseInfo
    {
        public short ErrorCode { get; set; }
        public string GroupId { get; set; }
        public string State { get; set; }
        public string ProtocolType { get; set; }
        public string Protocol { get; set; }

        public List<DescribeGroupsResponseMemberInfo> Members { get; set; }

        public DescribeGroupsResponseInfo(short errorCode, string groupId, string state, string protocolType,
            string protocol, IEnumerable<DescribeGroupsResponseMemberInfo> memberInfos)
        {
            this.ErrorCode = errorCode;
            this.GroupId = groupId;
            this.State = state;
            this.ProtocolType = protocolType;
            this.Protocol = protocol;
            this.Members = memberInfos.ToList();
        }
    }
}