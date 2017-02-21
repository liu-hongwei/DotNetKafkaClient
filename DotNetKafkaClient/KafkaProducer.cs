using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DotNetKafkaClient
{
    public class KafkaProducer : IDisposable
    {
        private log4net.ILog logger = log4net.LogManager.GetLogger(typeof(KafkaProducer).Name);
        private KafkaConfiguration config;
        private Dictionary<int, Broker> brokers = new Dictionary<int, Broker>();
        private Dictionary<int, Socket>  sockets = new Dictionary<int, Socket>();
        Dictionary<int, Dictionary<string, List<TopicMetadataResponsePartitionInfo>>> brokerOrderedMetadatas = new Dictionary<int, Dictionary<string, List<TopicMetadataResponsePartitionInfo>>>();

        private Thread metadataThread;
        private bool stopRunning;
        public KafkaProducer(KafkaConfiguration producerConfig)
        {
            this.config = producerConfig;

            if (producerConfig.KafkaBrokers == null || producerConfig.KafkaBrokers.Any() == false)
            {
                throw new ArgumentException("kafka broker list is null or empty.");
            }

            this.brokers = new Dictionary<int, Broker>();
            var id = 1;
            foreach (var bk in config.KafkaBrokers)
            {
                if (!brokers.ContainsKey(id))
                {
                    brokers.Add(id, new Broker(id, bk.Address.ToString(), bk.Port));

                    id++;
                }
            }

            // create sockets for each brokers
            this.sockets = new Dictionary<int, Socket>();
            foreach (var bk in brokers)
            {
                if (!sockets.ContainsKey(bk.Key))
                {
                    var broker = bk.Value;
                    IPEndPoint ipe = new IPEndPoint(IPAddress.Parse(broker.Host), broker.Port);

                    Socket socket = new Socket(ipe.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
                    {
                        NoDelay = true,
                        ReceiveTimeout = this.config.ReceiveTimeout,
                        SendTimeout = this.config.SendTimeout,
                        SendBufferSize = this.config.SendBufferSize,
                        ReceiveBufferSize = this.config.ReceiveBufferSize
                    };
                    socket.Connect(ipe);

                    if (!sockets.ContainsKey(bk.Key))
                    {
                        sockets.Add(bk.Key, socket);
                    }
                    else
                    {
                        sockets[bk.Key] = socket;
                    }
                }
            }

            this.stopRunning = false;

            // start thread to refresh the metadata
            metadataThread = new Thread(metadataRefreshThread);
            metadataThread.Start();
        }

        private void metadataRefreshThread()
        {
            int refreshInterval = 30 * 1000;

            while (!this.stopRunning)
            {
                try
                {
                    this.metadataAction();
                }
                catch (Exception e)
                {
                    logger.Debug("[UNHANDLED-EXCEPTION]coordinatorRefreshThread : " + e);
                }

                Thread.Sleep(refreshInterval);
            }
        }
        void metadataAction()
        {
            var metadataReq = TopicMetadataRequest.Create(null, 0, 0, this.config.ConsumerId);
            var sendBytes = metadataReq.Serialize();
            var bk = sockets.First();
            var bid = bk.Key;
            var socket = bk.Value;
            IEnumerable<TopicMetadataResponseTopicInfo> response = null;

            DateTime startUtc = DateTime.UtcNow;

                logger.Debug("metadataAction => lock on socket wait for " + bid);

                lock (socket)
                {
                    logger.Debug("metadataAction => lock on socket acquired " + bid + " in " +
                                 TimeSpan.FromTicks(DateTime.UtcNow.Ticks - startUtc.Ticks).TotalSeconds + " seconds");
                    if (!socket.Connected)
                    {
                        socket.Connect(socket.RemoteEndPoint);
                    }

                    NetworkStream stream = new NetworkStream(socket)
                    {
                        ReadTimeout = this.config.ReceiveTimeout,
                        WriteTimeout = this.config.SendTimeout
                    };
                    stream.Write(sendBytes, 0, sendBytes.Length);
                    stream.Flush();

                    var reader = new KafkaBinaryReader(stream);

                    // parse data
                    response = TopicMetadataRequest.ParseFrom(reader).TopicMetadatas;
                }
            

            if (response == null)
            {
                throw new Exception("[UNHANDLED-EXCEPTION] null MetadataResponse");
            }

            logger.Debug("metadataAction => metadata response from broker " + bid + " received in " +
                         TimeSpan.FromTicks(DateTime.UtcNow.Ticks - startUtc.Ticks).TotalSeconds + " seconds");

            brokerOrderedMetadatas.Clear();

            foreach (var metadata in response)
            {
                foreach (var p in metadata.PartitionsMetadata)
                {
                    logger.Debug("metadataAction => topic:" + metadata.Topic + ",partition:" + p.PartitionId + ",leader" +
                                 p.Leader.Id);

                    if (!brokerOrderedMetadatas.ContainsKey(p.Leader.Id))
                    {
                        brokerOrderedMetadatas.Add(p.Leader.Id,
                            new Dictionary<string, List<TopicMetadataResponsePartitionInfo>>());
                    }
                    if (!brokerOrderedMetadatas[p.Leader.Id].ContainsKey(metadata.Topic))
                    {
                        brokerOrderedMetadatas[p.Leader.Id].Add(metadata.Topic, new List<TopicMetadataResponsePartitionInfo>());
                    }

                    brokerOrderedMetadatas[p.Leader.Id][metadata.Topic].Add(p);
                }
            }

        }

        public void SendMessage(string topic, int partition, string key, string data)
        {
            // 1. figure out who owns the topic and partition
            var target = this.brokerOrderedMetadatas.FirstOrDefault(x => x.Value.Any(y => y.Key == topic && y.Value.Any(z => z.PartitionId == partition)));
            var bkId = target.Key;
            var socket = this.sockets[bkId];

            var request = new KafkaProduceRequest();
            
            var bytesSend = request.Serialize();

            KafkaProduceResponse response = null;
            // 2. send the request to the target brokers
            lock (socket)
            {
                if (!socket.Connected)
                {
                    socket.Connect(socket.RemoteEndPoint);
                }
                var stream = new NetworkStream(socket)
                {
                    ReadTimeout = this.config.ReceiveTimeout,
                    WriteTimeout = this.config.SendTimeout
                };

                stream.Write(bytesSend, 0, bytesSend.Length);
                var reader = new KafkaBinaryReader(stream);
                response = KafkaProduceResponse.ParseFrom(reader);
            }

            // check response
        }

        public void Dispose()
        {
            this.stopRunning = true;
            if (this.metadataThread != null && this.metadataThread.IsAlive)
            {
                this.metadataThread.Abort();
            }
        }
    }

  
    public class KafkaProduceRequest : KafkaRequest
    {
        public override int Size { get; set; }
        public override int CorrelationId { get; set; }
        public override ApiKey ApiKey {
            get
            {
                return ApiKey.Produce;
            }
        }
        public override short ApiVersion { get; set; }
        public override string ClientId { get; set; }

        public short RequiredAcks { get; set; }

        public int Timeout { get; set; }

        public KafkaProduceRequestTopicInfo [] TopicInfos { get; set; }

        public override byte[] Serialize()
        {
            throw new NotImplementedException();
        }
    }

    public class KafkaProduceRequestTopicInfo
    {
        public string Topic { get; set; }

        public KafkaProduceRequestPartitionInfo [] PartitionInfos { get; set; }
    }

    public class KafkaProduceRequestPartitionInfo
    {
        public int Partition { get; set; }

        public int MessageSetSize { get; set; }

        public KafkaMessageSet[] MessageSet { get; set; }
    }

    public class KafkaMessageSet
    {
        public long Offset { get; set; }
        public int MessageSize { get; set; }
        public KafkaMessage Message { get; set; }
    }

    public class KafkaProduceResponse : KafkaResponse
    {
        public override int Size { get; set; }
        public override int CorrelationId { get; set; }

        public KafkaProduceResponseTopicInfo [] TopicInfos { get; set; }

        // supported in 0.9.0 or later
        public int ThrottleTime { get; set; }

        public static KafkaProduceResponse ParseFrom(KafkaBinaryReader reader)
        {
            return new KafkaProduceResponse();
        }
    }

    public class KafkaProduceResponseTopicInfo
    {
        public string Topic { get; set; }

        public KafkaProduceResponsePartitionInfo [] PartitionInfos { get; set; }
    }

    public class KafkaProduceResponsePartitionInfo
    {
        public int Partition { get; set; }
        public short ErrorCode { get; set; }

        public long Offset { get; set; }

        // supported in 0.10.0 or later
        public long Timestamp { get; set; }
    }

}
