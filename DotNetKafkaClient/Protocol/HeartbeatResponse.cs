namespace DotNetKafkaClient
{
    public class HeartbeatResponse : KafkaResponse
    {
        public override int Size { get; set; }
        public override int CorrelationId { get; set; }

        /*
         HeartbeatResponse => ErrorCode
            ErrorCode => int16
         */

        public short ErrorCode { get; set; }

        public HeartbeatResponse(short errorCode)
        {
            this.ErrorCode = errorCode;
        }

        public static HeartbeatResponse ParseFrom(KafkaBinaryReader reader)
        {
            var size = reader.ReadInt32();
            var correlationid = reader.ReadInt32();
            var error = reader.ReadInt16();

            return new HeartbeatResponse(error);
        }


    }
}