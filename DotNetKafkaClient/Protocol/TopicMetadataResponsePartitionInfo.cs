using System.Collections.Generic;
using System.Linq;

namespace DotNetKafkaClient
{
    public class TopicMetadataResponsePartitionInfo
    {
        public const int DefaultPartitionIdSize = 4;
        public const int DefaultIfLeaderExistsSize = 1;
        public const int DefaultNumberOfReplicasSize = 2;
        public const int DefaultNumberOfSyncReplicasSize = 2;
        public const int DefaultIfLogSegmentMetadataExistsSize = 1;

        public TopicMetadataResponsePartitionInfo(int partitionId, Broker leader, IEnumerable<Broker> replicas,
            IEnumerable<Broker> isr)
        {
            this.PartitionId = partitionId;
            this.Leader = leader;
            this.Replicas = replicas;
            this.Isr = isr;
        }

        public int PartitionId { get; private set; }
        public Broker Leader { get; private set; }
        public IEnumerable<Broker> Replicas { get; private set; }
        public IEnumerable<Broker> Isr { get; private set; }
        public int SizeInBytes
        {
            get
            {
                var size = DefaultPartitionIdSize;
                if (this.Leader != null)
                {
                    size += this.Leader.SizeInBytes;
                }
                size += DefaultNumberOfReplicasSize;
                size += Replicas.Sum(replica => replica.SizeInBytes);
                size += DefaultNumberOfSyncReplicasSize;
                size += Isr.Sum(isr => isr.SizeInBytes);
                size += DefaultIfLogSegmentMetadataExistsSize;
                return size;
            }
        }
        public void WriteTo(KafkaBinaryWriter writer)
        {
            //Guard.NotNull(writer, "writer");

            // if leader exists
            writer.Write(this.PartitionId);
            if (this.Leader != null)
            {
                writer.Write((byte)1);
                this.Leader.WriteTo(writer);
            }
            else
            {
                writer.Write((byte)0);
            }

            // number of replicas
            writer.Write((short)Replicas.Count());
            foreach (var replica in Replicas)
            {
                replica.WriteTo(writer);
            }

            // number of in-sync replicas
            writer.Write((short)this.Isr.Count());
            foreach (var isr in Isr)
            {
                isr.WriteTo(writer);
            }

            writer.Write((byte)0);
        }

        public static TopicMetadataResponsePartitionInfo ParseFrom(KafkaBinaryReader reader, Dictionary<int, Broker> brokers)
        {
            var errorCode = reader.ReadInt16();
            var partitionId = reader.ReadInt32();
            var leaderId = reader.ReadInt32();
            Broker leader = null;
            if (leaderId != -1)
            {
                leader = brokers[leaderId];
            }

            // list of all replicas
            var numReplicas = reader.ReadInt32();
            var replicas = new List<Broker>();
            for (int i = 0; i < numReplicas; ++i)
            {
                var id = reader.ReadInt32();
                if (brokers.ContainsKey(id))
                {
                    replicas.Add(brokers[id]);
                }
            }

            // list of in-sync replicas
            var numIsr = reader.ReadInt32();
            var isrs = new List<Broker>();
            for (int i = 0; i < numIsr; ++i)
            {
                var id = reader.ReadInt32();
                if (brokers.ContainsKey(id))
                {
                    isrs.Add(brokers[id]);
                }
            }

            return new TopicMetadataResponsePartitionInfo(partitionId, leader, replicas, isrs);
        }
    }
}