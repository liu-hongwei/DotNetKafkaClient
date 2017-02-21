using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace DotNetKafkaClient
{
    public class JoinGroupRequestProtocolMetadataInfo
    {
        public short Version { get; set; }
        public List<string> Topics { get; set; } // topics
        public byte[] UserData { get; set; }

        public JoinGroupRequestProtocolMetadataInfo()
        {
        }

        public JoinGroupRequestProtocolMetadataInfo(short version, IEnumerable<string> topics, byte[] userData)
        {
            this.Version = version;
            this.Topics = topics.ToList();
            this.UserData = userData;
        }

        public byte[] Serialize()
        {
            byte[] bytes;
            using (var ms = new MemoryStream())
            {
                var writer = new KafkaBinaryWriter(ms);
                writer.Write(this.Version);
                if (this.Topics == null || this.Topics.Count == 0)
                {
                    writer.Write(0);
                }
                else
                {
                    writer.Write(this.Topics.Count);
                    for (int i = 0; i < this.Topics.Count; i++)
                    {
                        var topicbytes = Encoding.UTF8.GetBytes(this.Topics[i]);
                        writer.Write((short)topicbytes.Length);
                        writer.Write(topicbytes);
                    }
                }

                if (this.UserData == null || this.UserData.Length == 0)
                {
                    writer.Write(0);
                }
                else
                {
                    writer.Write(this.UserData.Length);
                    writer.Write(this.UserData);
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
                var topics = new string[count];
                for (var i = 0; i < count; i++)
                {
                    var length = reader.ReadInt16();
                    var topic = reader.ReadBytes(length);
                    topics[i] = Encoding.UTF8.GetString(topic);
                }
                this.Topics = new List<string>(topics);

                count = reader.ReadInt32();
                this.UserData = reader.ReadBytes(count);
            }
        }
    }
}