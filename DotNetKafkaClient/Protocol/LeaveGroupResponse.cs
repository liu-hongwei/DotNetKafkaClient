namespace DotNetKafkaClient
{
    public class LeaveGroupResponse : KafkaResponse
    {

        public override int Size { get;  set; }
        public override int CorrelationId { get;  set; }

        /*
         LeaveGroupResponse => ErrorCode
            ErrorCode => int16
         */

        public short ErrorCode { get; set; }

        public LeaveGroupResponse(short errorCode)
        {
            this.ErrorCode = errorCode;

        }

        public static LeaveGroupResponse ParseFrom(KafkaBinaryReader reader)
        {
            var size = reader.ReadInt32();
            var correlationid = reader.ReadInt32();
            var error = reader.ReadInt16();
            return new LeaveGroupResponse(error);
        }

      
    }
}