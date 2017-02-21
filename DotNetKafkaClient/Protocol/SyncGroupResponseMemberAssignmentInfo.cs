using System.IO;
using System.Text;

namespace DotNetKafkaClient
{
    public class SyncGroupResponseMemberAssignmentInfo
    {
        public short Version { get; set; }
        public SyncGroupResponsePartitionAssignmentInfo[] PartitionAssignmentInfos { get; set; }
        public byte[] UserData { get; set; }

        public byte[] Serialize()
        {
            byte[] bytes;
            // serialize this object
            using (var ms = new MemoryStream())
            {
                var writer = new KafkaBinaryWriter(ms);
                writer.Write(this.Version);
                if (this.PartitionAssignmentInfos != null && this.PartitionAssignmentInfos.Length > 0)
                {
                    writer.Write(this.PartitionAssignmentInfos.Length);
                    foreach (var pi in this.PartitionAssignmentInfos)
                    {
                        if (string.IsNullOrEmpty(pi.Topic))
                        {
                            writer.Write((short)0);
                        }
                        else
                        {
                            writer.Write((short)pi.Topic.Length);
                            writer.Write(Encoding.UTF8.GetBytes(pi.Topic));
                        }

                        if (pi.Partitions == null || pi.Partitions.Length == 0)
                        {
                            writer.Write(0);
                        }
                        else
                        {
                            writer.Write(pi.Partitions.Length);
                            foreach (var ppi in pi.Partitions)
                            {
                                writer.Write(ppi);
                            }
                        }
                    }
                }
                else
                {
                    writer.Write(0);
                }

                if (this.UserData != null && this.UserData.Length > 0)
                {
                    writer.Write(this.UserData.Length);
                    writer.Write(this.UserData);
                }
                else
                {
                    writer.Write(0);
                }

                bytes = ms.ToArray();
            }

            return bytes;
        }

        public void Deserialize(byte[] bytes)
        {
            using (var ms = new MemoryStream(bytes))
            {
                var reader = new KafkaBinaryReader(ms);

                this.Version = reader.ReadInt16();
                var count = reader.ReadInt32();
                this.PartitionAssignmentInfos = new SyncGroupResponsePartitionAssignmentInfo[count];
                for (int i = 0; i < count; i++)
                {
                    this.PartitionAssignmentInfos[i] = new SyncGroupResponsePartitionAssignmentInfo();

                    var txtLen = reader.ReadInt16();
                    this.PartitionAssignmentInfos[i].Topic = Encoding.UTF8.GetString(reader.ReadBytes(txtLen));

                    var size = reader.ReadInt32();
                    this.PartitionAssignmentInfos[i].Partitions = new int[size];
                    for (int j = 0; j < size; j++)
                    {
                        var pid = reader.ReadInt32();
                        this.PartitionAssignmentInfos[i].Partitions[j] = pid;
                    }
                }

                var len = reader.ReadInt32();
                this.UserData = reader.ReadBytes(len);
            }
        }

    }
}