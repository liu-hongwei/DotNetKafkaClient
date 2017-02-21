using System;
using System.Runtime.Serialization;

namespace DotNetKafkaClient
{
    public class IllegalStateException : Exception
    {
        public IllegalStateException()
            : base()
        {
        }

        public IllegalStateException(string message)
            : base(message)
        {
        }

        public IllegalStateException(SerializationInfo info, StreamingContext context) : base(info, context) { }
    }
}