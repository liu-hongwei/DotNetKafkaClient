using System.IO;
using System.Text;

namespace DotNetKafkaClient
{
    public class SyncGroupRequestGroupAssignmentInfo
    {
        public string MemberId { get; set; }
        public byte[] MemberAssignment { get; set; }

        public SyncGroupRequestGroupAssignmentInfo(string memberid, byte[] memberAssignment)
        {
            this.MemberId = memberid;
            this.MemberAssignment = memberAssignment;
        }

        public SyncGroupResponseMemberAssignmentInfo ParseMemberAssignment()
        {
            // deserialize the bytes
            var info = new SyncGroupResponseMemberAssignmentInfo();
            using (var ms = new MemoryStream(this.MemberAssignment))
            {
                var reader = new KafkaBinaryReader(ms);
                info.Version = reader.ReadInt16();

                int count = reader.ReadInt32();
                info.PartitionAssignmentInfos = new SyncGroupResponsePartitionAssignmentInfo[count];
                for (int i = 0; i < count; i++)
                {
                    info.PartitionAssignmentInfos[i] = new SyncGroupResponsePartitionAssignmentInfo();

                    short txtSize = reader.ReadInt16();
                    byte[] txtBytes = reader.ReadBytes(txtSize);
                    info.PartitionAssignmentInfos[i].Topic = Encoding.UTF8.GetString(txtBytes);

                    int psize = reader.ReadInt32();
                    info.PartitionAssignmentInfos[i].Partitions = new int[psize];
                    for (int j = 0; j < psize; j++)
                    {
                        int pid = reader.ReadInt32();
                        info.PartitionAssignmentInfos[i].Partitions[j] = pid;
                    }
                }

                int bytesSize = reader.ReadInt32();
                info.UserData = reader.ReadBytes(bytesSize);
            }

            return info;

        }

        public byte[] Serialize(SyncGroupResponseMemberAssignmentInfo assignment)
        {
            return null;
        }
    }
}