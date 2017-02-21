using System;
using System.Collections.Generic;
using System.Net;

namespace DotNetKafkaClient
{
    public class KafkaConfiguration
    {
        public const short DefaultNumberOfTries = 2;

        public const int DefaultTimeout = -1;

        public const int DefaultShutdownTimeout = 10000;

        public const bool DefaultAutoCommit = true;

        // java driver default auto.comit.interval.ms is 60*1000
        public const int DefaultAutoCommitInterval = /*10*/ 5 * 1000;

        //java driver default fetch.message.max.bytes is 1024*1024 = 1M
        public const int DefaultFetchSize = 1 * 1024 * 1024;

        //fetch.min.bytes
        public const int DefaultFetchMinBytes = 1;

        //java driver default fetch.wait.max.ms is 100
        public const int DefaultMaxFetchWaitMs = 100;

        public const int DefaultMaxFetchFactor = 10;

        public const int DefaultBackOffIncrement = 1000;

        // .net NetworkStream default SendTimeout and ReceiveTimeout are 0.
        public const int DefaultSocketTimeout = 30 * 1000;

        //kafka broker default socket.receive.buffer.bytes is 102400=100*1024=100K
        // .net Socket default ReceiveBufferSize is 8192=8*1024=8K
        public const int DefaultReceiveBufferSize = 100 * 1024;

        // in java driver default socket send buffer 64*1024=64K
        // .net Socket default SendBufferSize is 8192=8*1024=8K
        public const int DefaultSendBufferSize = 128 * 1024;

        public const int DefaultSendTimeout = 5 * 1000;

        public const int DefaultReceiveTimeout = 5 * 1000;

        public const int DefaultReconnectInterval = 60 * 1000;

        public const string DefaultConsumerId = null;

        public const string DefaultSection = "kafkaConsumer";

        public const int DefaultMaxFetchBufferLength = 1000;

        public const int DefaultConsumeGroupRebalanceRetryIntervalMs = 1000;

        public const int DefaultConsumeGroupFindNewLeaderSleepIntervalMs = 2000;


        public KafkaConfiguration()
        {
            this.NumberOfTries = DefaultNumberOfTries;
            this.Timeout = DefaultTimeout;
            this.AutoOffsetReset = OffsetRequest.SmallestTime;
            this.AutoCommit = DefaultAutoCommit;
            this.AutoCommitInterval = DefaultAutoCommitInterval;
            this.FetchSize = DefaultFetchSize;
            this.FetchMinBytes = DefaultFetchMinBytes;
            this.MaxFetchWaitMs = DefaultMaxFetchWaitMs;
            //this.MaxFetchFactor = DefaultMaxFetchFactor;
            this.BackOffIncrement = DefaultBackOffIncrement;
            this.ConsumerId = GetHostName();
            this.ReconnectInterval = DefaultReconnectInterval;
            this.ShutdownTimeout = DefaultShutdownTimeout;
            this.MaxFetchBufferLength = DefaultMaxFetchBufferLength;
            this.SendTimeout = DefaultSocketTimeout;
            this.ReceiveTimeout = DefaultSocketTimeout;
            this.BufferSize = DefaultReceiveBufferSize;
            this.SendBufferSize = DefaultSendBufferSize;
            this.ReceiveBufferSize = DefaultReceiveBufferSize;
            this.Verbose = false;
            this.ConsumeGroupRebalanceRetryIntervalMs = DefaultConsumeGroupRebalanceRetryIntervalMs;
            this.ConsumeGroupFindNewLeaderSleepIntervalMs = DefaultConsumeGroupFindNewLeaderSleepIntervalMs;

            // by default commit offset to zookeeper
            this.CommitOffsetToBroker = true;
            this.DotNotCommitOffset = false;
            this.BalanceInGroupMode = true;

            ////this.UseAsyncSocket = false;
        }


        /// <summary>
        /// The number of retry for get response.
        /// Default value: 2
        /// </summary>
        public short NumberOfTries { get; set; }

        /// <summary>
        /// The Socket send timeout. in milliseconds.
        /// Default value 30*1000
        /// </summary>
        public int SendTimeout { get; set; }

        /// <summary>
        /// The Socket recieve time out. in milliseconds.
        /// Default value 30*1000
        /// </summary>
        public int ReceiveTimeout { get; set; }

        /// <summary>
        /// The Socket reconnect interval. in milliseconds.
        /// Default value 60*1000
        /// </summary>
        public int ReconnectInterval { get; set; }

        /// <summary>
        /// The socket recieve / send buffer size. in bytes.
        /// Map to socket.receive.buffer.bytes in java api.
        /// Default value 11 * 1024 * 1024
        /// java version original default  value: 64 *1024
        /// </summary>
        public int BufferSize { get; set; }

        public int SendBufferSize { get; set; }
        public int ReceiveBufferSize { get; set; }

        /// <summary>
        /// Broker:  BrokerID, Host, Port
        /// </summary>
        public Broker Broker { get; set; }

        /// <summary>
        /// Log level is verbose or not
        /// </summary>
        public bool Verbose { get; set; }

        /// <summary>
        /// Consumer group API only.
        /// the number of byes of messages to attempt to fetch. 
        /// map to fetch.message.max.bytes of java version.
        /// Finally it call FileChannle.position(long newPosition)  and got to native call position0(FileDescriptor fd, long offset)
        /// Default value: 11 * 1024*1024
        /// </summary>
        public int FetchSize { get; set; }

        /// <summary>
        /// fetch.min.bytes -
        /// The minimum amount of data the server should return for a fetch request. If insufficient data is available the request will wait for that much data to accumulate before answering the request.
        /// Default value: 1
        /// </summary>
        public int FetchMinBytes { get; set; }

        /// <summary>
        /// fetch.wait.max.ms -
        /// The maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy fetch.min.bytes.
        /// Default value: 100
        /// </summary>
        public int MaxFetchWaitMs { get; set; }

        /// <summary>
        /// Consumer Group API only. Zookeeper
        /// </summary>
        ////public ZooKeeperConfiguration ZooKeeper { get; set; }

        public IPEndPoint [] KafkaBrokers { get; set; }

        /// <summary>
        /// Consumer Group API only.  The group name of consumer group, should not be empty.
        /// </summary>
        public string GroupId { get; set; }

        /// <summary>
        /// Consumer Group API only. The time out of get data from the BlockingCollection of KafkaMessageStream. in milliseconds.
        /// If the value less than 0, it will block there is no data available.
        /// If the value bigger of equal than 0 and got time out , one ConsumerTimeoutException will be thrown.
        /// Default value: -1
        /// </summary>
        public int Timeout { get; set; }

        /// <summary>
        /// Consumer Group API only.  The time out of shutting down fetcher thread. in milliseconds.
        /// Default value: 10,000
        /// </summary>
        public int ShutdownTimeout { get; set; }

        /// <summary>
        /// Consumer Group API only. Where to reset offset after got ErrorMapping.OffsetOutOfRangeCode.
        /// Valid value: OffsetRequest.SmallestTime  or OffsetRequest.LargestTime
        /// Default value: OffsetRequest.SmallestTime
        /// </summary>
        public string AutoOffsetReset { get; set; }

        /// <summary>
        /// Consumer Group API only.  Automate commit offset or not.
        /// Default value: true
        /// </summary>
        public bool AutoCommit { get; set; }

        /// <summary>
        /// Consumer Group API only.  The interval of commit offset. in milliseconds.
        /// Default value: 10,000
        /// </summary>
        public int AutoCommitInterval { get; set; }

        public bool BalanceInGroupMode { get; set; }

        public bool DotNotCommitOffset { get; set; }

        public Dictionary<string, Dictionary<int, long>> StartingOffsets { get; set; }

        public bool CommitOffsetToBroker { get; set; }

        /// <summary>
        /// Consumer Group API only. The count of message trigger fetcher thread cache, if the message count in fetch thread less than it, it will try fetch more from kafka.
        /// Default value: 1000
        /// Should be : (5~10 even 100) *  FetchSize / average message size  
        /// If this value set too big, your exe will use more memory to cache data.
        /// If this valuse set too small, your exe will raise more request to Kafka. 
        /// </summary>
        public int MaxFetchBufferLength { get; set; }

        /// <summary>
        /// Consumer Group API only.  the time of sleep when no data to fetch. in milliseconds.
        /// Default value: 1000
        /// </summary>
        public int BackOffIncrement { get; set; }

        /// <summary>
        /// Consumer group only. 
        /// Default value: host name
        /// </summary>
        public string ConsumerId
        {
            get
            {
                return consumerId;
            }
            set
            {
                //append ticks, so that consumerId is unqique, but sequential
                //non-unique consumerId may lead to issues, when broker loses connection and restores it
                consumerId = string.IsNullOrEmpty(value) ? "consumer" + "-" + DateTime.UtcNow.Ticks : value;
            }
        }
        private string consumerId;

        /// <summary>
        /// Consumer group only.
        /// Default value : 1000 ms.
        /// </summary>
        public int ConsumeGroupRebalanceRetryIntervalMs { get; set; }

        private string GetHostName()
        {
            var shortHostName = Dns.GetHostName();
            var fullHostName = Dns.GetHostEntry(shortHostName).HostName;
            return fullHostName;
        }

        /// <summary>
        /// Consumer group only.
        /// Default value: 2000ms
        /// </summary>
        public int ConsumeGroupFindNewLeaderSleepIntervalMs { get; set; }
    }
}