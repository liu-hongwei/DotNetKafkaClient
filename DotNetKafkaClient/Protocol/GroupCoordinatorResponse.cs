namespace DotNetKafkaClient
{
    public class GroupCoordinatorResponse : KafkaResponse
    {
        public override int Size { get;  set; }
        public override int CorrelationId { get;  set; }

        /*
        GroupCoordinatorResponse => ErrorCode CoordinatorId CoordinatorHost CoordinatorPort
          ErrorCode => int16
          CoordinatorId => int32
          CoordinatorHost => string
          CoordinatorPort => int32
        */

        public short ErrorCode { get; set; }
        public int CoordinatorId { get; set; }
        public string CoordinatorHost { get; set; }
        public int CoordinatorPort { get; set; }


        public GroupCoordinatorResponse(short errorCode, int coordinatorId, string coordinatorHost, int coordinatorPort)
        {
            this.ErrorCode = errorCode;
            this.CoordinatorId = coordinatorId;
            this.CoordinatorHost = coordinatorHost;
            this.CoordinatorPort = coordinatorPort;
        }

        public static GroupCoordinatorResponse ParseFrom(KafkaBinaryReader reader)
        {
            var size = reader.ReadInt32();
            var correlationid = reader.ReadInt32();
            var error = reader.ReadInt16();
            var coordinatorid = reader.ReadInt32();
            var coordinatorhost = reader.ReadShortString();
            var coordinatorport = reader.ReadInt32();
            return new GroupCoordinatorResponse(error, coordinatorid, coordinatorhost, coordinatorport);
        }

        
    }
}