using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DotNetKafkaClient
{
    public abstract class KafkaResponse : KafkaPacket
    {
      //public static KafkaResponse ParseFrom(ApiKey apiKey, byte[] response)
      //  {
      //      switch (apiKey)
      //      {
      //          case ApiKey.DescribeGroups:
      //              return DescribeGroupsResponse.ParseFrom(new KafkaBinaryReader(new MemoryStream(response)));
      //              break;
      //      }

      //      return null;
      //  }
    }
}
