using System.IO;
using System.Text;

namespace DotNetKafkaClient
{
    public class SyncGroupResponse : KafkaResponse
    {
        public override int Size { get;  set; }
        public override int CorrelationId { get;  set; }

        /*
         SyncGroupResponse => ErrorCode MemberAssignment
        ErrorCode => int16
        MemberAssignment => bytes
         */

        public short ErrorCode { get; set; }

        public byte[] MemberAssignment { get; set; }

        public SyncGroupResponse(short errorCode, byte[] memberAssignment)
        {
            this.ErrorCode = errorCode;
            this.MemberAssignment = memberAssignment;
        }

        public SyncGroupResponseMemberAssignmentInfo ParseMemberAssignment()
        {
            if (this.MemberAssignment.Length <= 0)
            {
                return null;
            }

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

        public static SyncGroupResponse ParseFrom(KafkaBinaryReader reader)
        {
            var size = reader.ReadInt32();
            var correlationid = reader.ReadInt32();

            var error = reader.ReadInt16();
            var count = reader.ReadInt32();
            var data = reader.ReadBytes(count);

            return new SyncGroupResponse(error, data);
        }

      
    }
}