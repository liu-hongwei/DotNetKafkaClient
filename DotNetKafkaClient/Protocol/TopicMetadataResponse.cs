using System.Collections.Generic;

namespace DotNetKafkaClient
{
    public class TopicMetadataResponse
    {
        public IEnumerable<Broker> Brokers { get; set; }
        public IEnumerable<TopicMetadataResponseTopicInfo> TopicMetadatas { get; set; }
    }
}