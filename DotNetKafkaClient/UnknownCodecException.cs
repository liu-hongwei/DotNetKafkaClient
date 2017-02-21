using System;
using System.Runtime.Serialization;

namespace DotNetKafkaClient
{
    public class UnknownCodecException : Exception
    {
        public UnknownCodecException()
            : base()
        {
        }

        public UnknownCodecException(string message)
            : base(message)
        {
        }

        public UnknownCodecException(SerializationInfo info, StreamingContext context) : base(info, context) { }
    }
}