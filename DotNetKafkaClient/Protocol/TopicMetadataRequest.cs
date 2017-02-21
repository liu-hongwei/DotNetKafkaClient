using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DotNetKafkaClient
{
    public class TopicMetadataRequest : KafkaRequest
    {
        private const int DefaultNumberOfTopicsSize = 4;
        private const byte DefaultHeaderSize8 = DefaultRequestSizeSize + DefaultRequestIdSize;

        public override short ApiVersion { get; set; }
        public override int CorrelationId { get; set; }
        public override string ClientId { get; set; }

        private TopicMetadataRequest(IEnumerable<string> topics, short versionId, int correlationId, string clientId)
        {
            /*
            if (topics == null)
            {
                throw new ArgumentNullException("topics", "List of topics cannot be null.");
            }

            if (!topics.Any())
            {
                throw new ArgumentException("List of topics cannot be empty.");
            }
            */

            if (topics == null || !topics.Any())
            {
                this.Topics = new List<string>();
            }
            else
            {
                this.Topics = new List<string>(topics);
            }
            this.ApiVersion = versionId;
            this.CorrelationId = correlationId;
            this.ClientId = clientId;
        }

        public IEnumerable<string> Topics { get; private set; }
       
        /// <summary>
        /// Creates simple request with no segment metadata information
        /// </summary>
        /// <param name="topics">list of topics</param>
        /// <param name="versionId"></param>
        /// <param name="correlationId"></param>
        /// <param name="clientId"></param>
        /// <returns>request</returns>
        public static TopicMetadataRequest Create(IEnumerable<string> topics, short versionId, int correlationId, string clientId)
        {
            return new TopicMetadataRequest(topics, versionId, correlationId, clientId);
        }

        public override ApiKey ApiKey
        {
            get { return ApiKey.TopicMetadataRequest; }
        }

        public override byte[] Serialize()
        {
            using (var ms = new MemoryStream(this.Size))
            {
                using (var writer = new KafkaBinaryWriter(ms))
                {
                    writer.Write(ms.Capacity - DefaultRequestSizeSize);
                    writer.Write((short)this.ApiKey);
                    writer.Write(this.ApiVersion);
                    writer.Write(this.CorrelationId);
                    writer.WriteShortString(this.ClientId, DefaultEncoding);
                    writer.Write(this.Topics.Count());
                    foreach (var topic in Topics)
                    {
                        writer.WriteShortString(topic, DefaultEncoding);
                    }

                    return ms.GetBuffer();
                }
            }
        }
        

        public override int Size
        {
            get
            {

                return DefaultHeaderSize8 +
                       FetchRequest.DefaultApiVersionSize +
                       FetchRequest.DefaultCorrelationIdSize +
                       KafkaPrimitiveTypes.GetShortStringLength(this.ClientId, DefaultEncoding) +
                       DefaultNumberOfTopicsSize +
                       this.Topics.Sum(x => KafkaPrimitiveTypes.GetShortStringLength(x, DefaultEncoding));

            }
            set { }
        }


        public static TopicMetadataResponse ParseFrom(KafkaBinaryReader reader)
        {
            reader.ReadInt32();
            int correlationId = reader.ReadInt32();
            int brokerCount = reader.ReadInt32();
            var brokerMap = new Dictionary<int, Broker>();
            for (int i = 0; i < brokerCount; ++i)
            {
                var broker = Broker.ParseFrom(reader);
                brokerMap[broker.Id] = broker;
            }

            var numTopics = reader.ReadInt32();
            var topicMetadata = new TopicMetadataResponseTopicInfo[numTopics];
            for (int i = 0; i < numTopics; i++)
            {
                topicMetadata[i] = TopicMetadataResponseTopicInfo.ParseFrom(reader, brokerMap);
            }

            var response = new TopicMetadataResponse();
            response.Brokers = brokerMap.Select(x => x.Value);
            response.TopicMetadatas = topicMetadata;

            return response;
        }


      
    }
}