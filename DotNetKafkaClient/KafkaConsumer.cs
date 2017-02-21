using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;

namespace DotNetKafkaClient
{
    /*
     * https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetFetchRequest
        Overview
        The Kafka protocol is fairly simple, there are only six core client requests APIs.
        Metadata - Describes the currently available brokers, their host and port information, and gives information about which broker hosts which partitions.
        Send - Send messages to a broker
        Fetch - Fetch messages from a broker, one which fetches data, one which gets cluster metadata, and one which gets offset information about a topic.
        Offsets - Get information about the available offsets for a given topic partition.
        Offset Commit - Commit a set of offsets for a consumer group
        Offset Fetch - Fetch a set of offsets for a consumer group

        Each of these will be described in detail below. Additionally, as of 0.9, Kafka supports general group management for consumers and Kafka Connect. The client API consists of five requests:
        
        GroupCoordinator - Locate the current coordinator of a group.
        JoinGroup - Become a member of a group, creating it if there are no active members.
        SyncGroup - Synchronize state for all members of a group (e.g. distribute partition assignments to consumers).
        Heartbeat - Keep a member alive in the group. 
        LeaveGroup - Directly depart a group.
        Finally, there are several administrative APIs which can be used to monitor/administer the Kafka cluster (this list will grow when KIP-4 is completed).
        DescribeGroups - Used to inspect the current state of a set of groups (e.g. to view consumer partition assignments).  
        ListGroups - List the current groups managed by a broker.
     */


    public partial class KafkaConsumer : IDisposable
    {

        private log4net.ILog logger = log4net.LogManager.GetLogger(typeof(KafkaConsumer).Name);

        ConcurrentDictionary<int, List<KafkaFetchRequestEntry>> brokerOrderedFetchRequestEntries = new ConcurrentDictionary<int, List<KafkaFetchRequestEntry>>();
        Dictionary<int, Dictionary<string, List<TopicMetadataResponsePartitionInfo>>> brokerOrderedMetadatas = new Dictionary<int, Dictionary<string, List<TopicMetadataResponsePartitionInfo>>>();
        ConcurrentDictionary<string, ConcurrentQueue<KafkaMessage>> fetchedMessagesQueue = new ConcurrentDictionary<string, ConcurrentQueue<KafkaMessage>>();
        ConcurrentDictionary<string, List<KafkaMessage>> fetchedMessagesList = new ConcurrentDictionary<string, List<KafkaMessage>>();
        Dictionary<int, List<Task<object>>> taskQueue = new Dictionary<int, List<Task<object>>>();
        Dictionary<string, List<KeyValuePair<string, int>>> groupSyncRequestMap = new Dictionary<string, List<KeyValuePair<string, int>>>();

        string memberIdInGroup = "";
        int generationIdInGroup = -1;

        private Dictionary<int, Socket> sockets = new Dictionary<int, Socket>();
        private KafkaConfiguration config;
        List<string> _subscriptions = new List<string>();

        private bool needRebalance;
        private bool rebalanceComplete;
        private bool fetchEntitiesAssigned;
        private bool fetchOffsetAssigned;

        private Thread metadataThread;
        private ManualResetEvent metadataReadyEvent;
        private Thread coordinatorThread;
        private ManualResetEvent coordinatorReadyEvent;
        private Thread heartbeatingThread;
        private Thread autoCommitThread;

        private bool decodeResponseUsingQueue;
        ConcurrentQueue<KafkaRequestResponse> fetchResponseQueue = new ConcurrentQueue<KafkaRequestResponse>();
        private Thread fetchResponseQueueProcessThread;

        private Cluster cluster;
        Dictionary<int, Broker> brokers = new Dictionary<int, Broker>();
        private bool stopRunning;
        Dictionary<string, Dictionary<int, long>> _lastAutoCommittedOffset = new Dictionary<string, Dictionary<int, long>>();

        private bool useAsyncSocket;
        Dictionary<int, KafkaSocket> asyncSockets = new Dictionary<int, KafkaSocket>();

        private long uniqueId = 0;

        private object refreshLock;
        private int refreshInterval;
        private int coordinatorId;

        public KafkaConsumer(KafkaConfiguration config)
        {
            if (config.KafkaBrokers == null || config.KafkaBrokers.Any() == false)
            {
                throw new ArgumentException("kafka brokers list is null or empty.");
            }

            this.useAsyncSocket = false;
            this.config = config;


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

                if (this.useAsyncSocket)
                {
                    var broker = bk.Value;
                    IPEndPoint ipe = new IPEndPoint(IPAddress.Parse(broker.Host), broker.Port);
                    var asyncSocket = new KafkaSocket(ipe);
                    if (!this.asyncSockets.ContainsKey(bk.Key))
                    {
                        this.asyncSockets.Add(bk.Key, asyncSocket);
                    }
                }
            }

            if (this.config.AutoCommit)
            {
                autoCommitThread = new Thread(this.autoCommitOffsetsThread);
                autoCommitThread.Start();
            }

            this.stopRunning = false;

            if (this.config.BalanceInGroupMode)
            {
                this.needRebalance = true;
                this.rebalanceComplete = false;
            }
            else
            {
                this.needRebalance = false;
                this.rebalanceComplete = true;
            }

            this.fetchEntitiesAssigned = false;
            this.fetchOffsetAssigned = false;

            this.decodeResponseUsingQueue = false;
            if (this.decodeResponseUsingQueue)
            {
                fetchResponseQueueProcessThread = new Thread(this.fetchResponseHandlingThread);
                fetchResponseQueueProcessThread.Start();
            }

            // start dedicated thread to refresh metadata and coordinator
            refreshLock = new object();
            refreshInterval = 30 * 1000;
            metadataReadyEvent = new ManualResetEvent(false);

            metadataThread = new Thread(metadataRefreshThread);
            metadataThread.Start();

            coordinatorReadyEvent = new ManualResetEvent(false);
            coordinatorThread = new Thread(coordinatorRefreshThread);
            coordinatorThread.Start();

            this.heartbeatingThread = new Thread(this.heartbeatThread);
            this.heartbeatingThread.Start();
        }

        private void coordinatorRefreshThread()
        {
            while (!this.stopRunning)
            {
                try
                {
                    var id = this.findCoordinatorFunc();
                    lock (refreshLock)
                    {
                        coordinatorId = id;
                    }

                    coordinatorReadyEvent.Set();
                }
                catch (Exception e)
                {
                    logger.Debug("[UNHANDLED-EXCEPTION]coordinatorRefreshThread : " + e);
                }

                Thread.Sleep(refreshInterval);
            }
        }
        private void metadataRefreshThread()
        {
            while (!this.stopRunning)
            {
                try
                {
                    this.metadataAction();
                    metadataReadyEvent.Set();
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
            var metadataReq = TopicMetadataRequest.Create(this._subscriptions, 0, 0, this.config.ConsumerId);
            var sendBytes = metadataReq.Serialize();
            var bk = sockets.First();
            var bid = bk.Key;
            var socket = bk.Value;
            IEnumerable<TopicMetadataResponseTopicInfo> response = null;

            DateTime startUtc = DateTime.UtcNow;

            if (!this.useAsyncSocket)
            {
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
            }
            else
            {
                var asyncSocket = this.asyncSockets[bid];
                response = asyncSocket.Send(metadataReq);
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

        public Dictionary<int, Dictionary<string, List<TopicMetadataResponsePartitionInfo>>> GetTopicMetadata(string topic)
        {
            /*this.metadataAction();*/
            if (!metadataReadyEvent.WaitOne(TimeSpan.FromMinutes(5)))
            {
                this.metadataAction();
                metadataReadyEvent.Set();
            }
            var filtered = brokerOrderedMetadatas.Where(x => x.Value.ContainsKey(topic));
            return filtered.ToDictionary(x => x.Key, x => x.Value);
        }

        Dictionary<string, Dictionary<int, long>> dataOffsetFunc(long time)
        {
            Dictionary<string, Dictionary<int, long>> offsets = new Dictionary<string, Dictionary<int, long>>();

            /*
            if (!this.brokerOrderedMetadatas.Any())
            {
                this.metadataAction();
            }
            */
            if (!metadataReadyEvent.WaitOne(TimeSpan.FromMinutes(5)))
            {
                this.metadataAction();
                metadataReadyEvent.Set();
            }

            foreach (var kvp in brokerOrderedMetadatas)
            {
                var bid = kvp.Key;
                var mts = kvp.Value;

                var reqInfo = new Dictionary<string, List<PartitionOffsetRequestInfo>>();
                foreach (var mt in mts)
                {
                    var tp = mt.Key;
                    var pms = mt.Value;

                    if (!reqInfo.ContainsKey(tp))
                    {
                        reqInfo.Add(tp, new List<PartitionOffsetRequestInfo>());
                    }

                    foreach (var pm in pms)
                    {
                        /*
                            Used to ask for all messages before a certain time (ms). There are two special values. 
                            Specify -1 to receive the latest offset (i.e. the offset of the next coming message) 
                            and -2 to receive the earliest available offset. 
                            Note that because offsets are pulled in descending order, asking for the earliest offset will always return you a single element.                        
                        */

                        reqInfo[tp].Add(new PartitionOffsetRequestInfo(pm.PartitionId, time, 1));
                    }
                }

                DateTime startUtc = DateTime.UtcNow;
                var offsetReq = new OffsetRequest(reqInfo, 0, 0, this.config.ConsumerId, -1);
                var socket = sockets[bid];
                OffsetResponse offsetRsp = null;

                if (!this.useAsyncSocket)
                {
                    logger.Debug("dataOffsetFunc => lock on socket wait for " + bid);
                    lock (socket)
                    {
                        logger.Debug("dataOffsetFunc => lock on socket acquired " + bid + " in " + TimeSpan.FromTicks(DateTime.UtcNow.Ticks - startUtc.Ticks).TotalSeconds + " seconds");
                        if (!socket.Connected)
                        {
                            socket.Connect(socket.RemoteEndPoint);
                        }

                        NetworkStream stream = new NetworkStream(socket);
                        var sendBytes = offsetReq.Serialize();

                        stream.Write(sendBytes, 0, sendBytes.Length);
                        stream.Flush();
                        var reader = new KafkaBinaryReader(stream);
                        offsetRsp = OffsetResponse.ParseFrom(reader);
                    }
                }
                else
                {
                    var asyncSocket = this.asyncSockets[bid];
                    offsetRsp = asyncSocket.Send(offsetReq);
                }

                if (offsetRsp == null)
                {
                    throw new Exception("[UNHANDLED-EXCEPTION] initOffsetAction => null OffsetResponse");
                }

                logger.Debug("initOffsetAction => earliest offset response from broker " + bid + " in " +
                             TimeSpan.FromTicks(DateTime.UtcNow.Ticks - startUtc.Ticks).TotalSeconds +
                             " seconds");

                foreach (var e in offsetRsp.ResponseMap)
                {
                    foreach (var f in e.Value)
                    {
                        if (f.Error == 0)
                        {
                            if (!offsets.ContainsKey(e.Key))
                            {
                                offsets.Add(e.Key, new Dictionary<int, long>());
                            }

                            if (!offsets[e.Key].ContainsKey(f.PartitionId))
                            {
                                offsets[e.Key].Add(f.PartitionId, f.Offsets[0]);
                            }
                            else
                            {
                                offsets[e.Key][f.PartitionId] = f.Offsets[0];
                            }
                        }
                    }
                }

            }

            return offsets;
        }

        //public void FetchData(int brokerId, IEnumerable<KafkaFetchRequestEntry> requests)
        //{
        //    fetchDataAction(new KafkaFetchTaskArgs() { BrokerId = brokerId, FetchEntities = requests.ToList() });
        //}

        void fetchResponseDecodeAction()
        {
            try
            {
                KafkaRequestResponse reqResp;

                if (fetchResponseQueue.TryDequeue(out reqResp))
                {
                    var bid = reqResp.Broker;
                    var response = reqResp.Response;
                    var currentReqs = reqResp.Requests;
                    var originalReqs = brokerOrderedFetchRequestEntries[bid];
                    var refreshOffsetNeeded = false;

                    foreach (var rsp in response.TopicDataDict)
                    {
                        var tp = rsp.Key;
                        var pds = rsp.Value;

                        if (!fetchedMessagesList.ContainsKey(tp))
                        {
                            fetchedMessagesList.TryAdd(tp, new List<KafkaMessage>());
                        }

                        foreach (var pditem in pds.PartitionData)
                        {
                            var pd = pditem;
                            long lastParsedOffst = 0;
                            double messageCounts = 0;
                            var error = (short)pd.Error;

                            // if the Position is changed by SetOffset, mean no need to handle previous response
                            var originalReq = originalReqs.First(x => x.BrokerId == bid && x.Topic == tp && x.PartitionId == pd.Partition);
                            if (originalReq == null)
                            {
                                logger.Debug("fetchResponseDecodeAction => original requests to broker " + bid + " not contain topic " + tp + " partition " + pd.Partition);
                            }

                            if (error != 0)
                            {
                                // possible error in fetch response
                                // 1 : offset out of range
                                // 3 : unknown topic or partition
                                // 6 : not leader for partition
                                // 9 : replica not available
                                // -1: unknown 

                                // in order to get the correct fetch postion, set it to 0, so next call will use earilest offset
                                originalReq.Position = 0;
                                refreshOffsetNeeded = true;
                                continue;
                            }

                            var currentReq = currentReqs.First(x => x.BrokerId == bid && x.Topic == tp && x.PartitionId == pd.Partition);
                            if (currentReq == null)
                            {
                                logger.Debug("fetchResponseDecodeAction => current requests to broker " + bid + " not contain topic " + tp + " partition " + pd.Partition);
                            }

                            if (originalReq == null || currentReq == null)
                            {
                                continue;
                            }

                            if (currentReq.Position != originalReq.Position)
                            {
                                logger.Debug("fetchResponseDecodeAction => position of fetch request to broker " + bid + " topic " + tp + " partition " + pd.Partition + " changed from " + originalReq.Position + " to " + currentReq.Position + " , discarding response from task id " + reqResp.ReqTaskId);
                                continue;
                            }

                            if (pd.RecordSet == null || pd.RecordSet.Length <= 0)
                            {
                                logger.Debug("fetchResponseDecodeAction => record set in response from broker " + bid + " topic " + tp + " partition " + pd.Partition + " is null or empty from task id " + reqResp.ReqTaskId);
                                continue;
                            }

                            using (var messageSet = new KafkaMessageSet(pd.Partition, pd.RecordSet))
                            {
                                try
                                {
                                    var list = fetchedMessagesList[tp];

                                    foreach (var message in messageSet)
                                    {
                                        if (message != null)
                                        {
                                            lock (list)
                                            {
                                                list.Add(message);
                                            }

                                            // offset shoud be increasing
                                            if (lastParsedOffst < message.Offset)
                                            {
                                                lastParsedOffst = message.Offset;
                                            }

                                            messageCounts++;
                                        }
                                    }
                                }
                                catch (Exception e)
                                {
                                    logger.Debug(
                                        "fetchResponseDecodeAction : KafkaMessageSet failed to decode messages " +
                                        e.Message);
                                }
                            }

                            // update fetch offset
                            if (lastParsedOffst > 0)
                            {
                                var lastOffset = lastParsedOffst + 1;
                                logger.Debug("fetchResponseDecodeAction => next fetch offset for partition " +
                                             pd.Partition + " should be " + lastOffset);
                                if (brokerOrderedFetchRequestEntries.ContainsKey(bid))
                                {
                                    var allpartitions = brokerOrderedFetchRequestEntries[bid];
                                    lock (allpartitions)
                                    {
                                        var target = allpartitions
                                            .First(
                                                x => x.BrokerId == bid && x.Topic == tp && x.PartitionId == pd.Partition);

                                        target.Position = lastOffset;

                                        logger.Debug("fetchResponseDecodeAction => next fetch offset for partition " +
                                                     pd.Partition + " is set to " + lastOffset +
                                                     " , current batch message count " + messageCounts);
                                    }
                                }
                            }
                        }
                    }

                    if (refreshOffsetNeeded)
                    {
                        this.fetchOffsetAssigned = false;
                    }

                    logger.Debug("fetchResponseDecodeAction => fetch response queue size now " + fetchResponseQueue.Count);
                }
                else
                {
                    logger.Debug("fetchResponseDecodeAction => no fetch response dequeued, current counts " + fetchResponseQueue.Count);
                }
            }
            catch (Exception e)
            {
                logger.Debug("fetchResponseDecodeAction : failed to process response " + e.Message);
            }
        }

        private void fetchResponseHandlingThread()
        {
            while (!this.stopRunning)
            {
                try
                {
                    this.fetchResponseDecodeAction();
                }
                catch (Exception e)
                {
                    logger.Debug("responseQueueThread : failed to process response " + e.Message);
                }
            }
        }

        void fetchDataAction(KafkaFetchTaskArgs o)
        {
            var oo = (KafkaFetchTaskArgs)o;
            var taskId = oo.FetchTaskId;
            var bid = oo.BrokerId;
            var entities = oo.FetchEntities;

            if (entities.Any(e => e.Position == -1))
            {
                this.ensureFetchOffsets();
            }
            
            Dictionary<string, List<FetchRequestPartitionInfo>> fetchInfos = new Dictionary<string, List<FetchRequestPartitionInfo>>();
            foreach (var entity in entities)
            {
                if (!fetchInfos.ContainsKey(entity.Topic))
                {
                    fetchInfos.Add(entity.Topic, new List<FetchRequestPartitionInfo>());
                }

                fetchInfos[entity.Topic].Add(new FetchRequestPartitionInfo(entity.PartitionId, entity.Position, this.config.FetchSize));
                

                logger.Debug("fetchAction => data fetch from broker " + bid + " topic " + entity.Topic + " partition " + entity.PartitionId + " fetch offset " + entity.Position);
            }

            var fetchRequest = new FetchRequest(0, this.config.ConsumerId, KafkaConfiguration.DefaultMaxFetchWaitMs, KafkaConfiguration.DefaultFetchMinBytes, fetchInfos);
            var sendBytes = fetchRequest.Serialize();
            FetchResponse fetchResponse = null;

            DateTime startUtc = DateTime.UtcNow;

            var socket = sockets[bid];
            bool useV2Parse = true;

            if (!this.useAsyncSocket)
            {
                logger.Debug("fetchDataAction => lock on socket wait for " + bid);
                lock (socket)
                {
                    logger.Debug("fetchDataAction => lock on socket acquired " + bid + " in " + TimeSpan.FromTicks(DateTime.UtcNow.Ticks - startUtc.Ticks).TotalSeconds + " seconds");

                    if (!socket.Connected)
                    {
                        socket.Connect(socket.RemoteEndPoint);
                    }

                    logger.Debug("***performance*** fetchAction socket connected for broker " + bid + " after " +
                                 TimeSpan.FromTicks(DateTime.UtcNow.Ticks - startUtc.Ticks).TotalSeconds + " seconds");

                    startUtc = DateTime.UtcNow;
                    var stream = new NetworkStream(socket)
                    {
                        ReadTimeout = this.config.ReceiveTimeout,
                        WriteTimeout = this.config.SendTimeout
                    };
                    stream.ReadTimeout = 60 * 1000;
                    stream.WriteTimeout = 60 * 1000;

                    stream.Write(sendBytes, 0, sendBytes.Length);

                    logger.Debug("***performance*** fetchAction request sent for broker " + bid + " after " +
                                 TimeSpan.FromTicks(DateTime.UtcNow.Ticks - startUtc.Ticks).TotalSeconds + " seconds");

                    startUtc = DateTime.UtcNow;

                    var reader = new KafkaBinaryReader(stream);

                    if (useV2Parse)
                    {
                        fetchResponse = FetchResponse.ParseFrom(reader);
                    }
                    else
                    {
                        fetchResponse = FetchResponse.ParseFrom(reader);
                    }
                }
            }
            else
            {
                logger.Debug("fetchAction => async socket sent request to broker " + bid);
                var asyncSocket = this.asyncSockets[bid];
                fetchResponse = asyncSocket.Send(fetchRequest);
            }

            if (fetchResponse == null)
            {
                throw new Exception("[UNHANDLED-EXCEPTION] fetchAction => null FetchResponse");
            }

            logger.Debug("***performance*** fetchAction => data fetch response from broker " + bid + " received in " +
                         TimeSpan.FromTicks(DateTime.UtcNow.Ticks - startUtc.Ticks).TotalSeconds + " seconds");


            // put the response to processing queue for handling
            this.fetchResponseQueue.Enqueue(new KafkaRequestResponse()
            {
                Broker = bid,
                ReqTaskId = taskId,
                Requests = entities,
                Response = fetchResponse
            });

            /*
            startUtc = DateTime.UtcNow;
            long total = 0;

            List<Task> decodeTasks = new List<Task>();

            foreach (var rsp in fetchResponse.TopicDataDict)
            {
                var tp = rsp.Key;
                var pds = rsp.Value;

                if (!fetchedMessagesQueue.ContainsKey(tp))
                {
                    fetchedMessagesQueue.TryAdd(tp, new ConcurrentQueue<Message>());
                }

                foreach (var pditem in pds.PartitionData)
                {
                    var pd = pditem;

                    //var task = Task.Factory.StartNew(() =>
                    //{
                        DateTime timer = DateTime.UtcNow;
                        List<Message> parsedMessages = new List<Message>();
                        var messageSet = new KafkaMessageSet(pd.Partition, pd.RecordSet);
                    
                        try
                        {
                          
                            foreach (var message in messageSet)
                            {
                                if (message != null)
                                {
                                    fetchedMessagesQueue[tp].Enqueue(message);
                                    parsedMessages.Add(message);
                                }
                            }

                            //var all = messageSet.ParseAll(pd.RecordSet);
                            //foreach (var message in all)
                            //{
                            //    if (message != null)
                            //    {
                            //        fetchedMessagesQueue[tp].Enqueue(message);
                            //        parsedMessages.Add(message);
                            //    }
                            //}
                        }
                        catch (Exception e)
                        {
                            logger.Debug("[UNHANDLED-EXCEPTION]fetchAction => failed to parse records for partition " + pd.Partition);
                            throw e;
                        }

                        logger.Debug("***performance*** fetchAction => MessageSet are enqueued for partition " + pd.Partition + ", total " + parsedMessages.Count + " in " +
                         TimeSpan.FromTicks(DateTime.UtcNow.Ticks - timer.Ticks).TotalSeconds + " seconds");
                    //    return parsedMessages;
                    //});

                    //decodeTasks.Add(task);

                    //task.ContinueWith(t =>
                    //{
                    //    decodeTasks.Remove(t);

                    //    var parsedMessages = t.Result;

                        // update fetch offset
                        var lastOffset = parsedMessages.Last().Offset + 1;
                        logger.Debug("fetchAction => next fetch offset for partition " + pd.Partition + " should be " + lastOffset);

                        if (brokerOrderedFetchRequestEntries.ContainsKey(bid))
                        {
                            var allpartitions = brokerOrderedFetchRequestEntries[bid];
                            lock (allpartitions)
                            {
                                var target = allpartitions
                                    .First(
                                        x => x.BrokerId == bid && x.Topic == tp && x.PartitionId == pd.Partition);

                                target.Position = lastOffset;

                                logger.Debug("fetchAction => next fetch offset for partition " + pd.Partition + " is set to " + lastOffset + " , current batch message count " + parsedMessages.Count);
                            }
                        }
                    //});

                }
            }
            */

            //Task.WaitAll(decodeTasks.ToArray());
            //Task.WaitAny(decodeTasks.ToArray());

            //Task.WhenAll(decodeTasks).ContinueWith(t =>
            //{
            logger.Debug("***performance*** fetchAction parse response from broker " + bid + " completed in " +
                         TimeSpan.FromTicks(DateTime.UtcNow.Ticks - startUtc.Ticks).TotalSeconds + " seconds");
            //});
        }

        private class KafkaMessageSet : IEnumerable<KafkaMessage>, IDisposable
        {
            private int partitionId;
            private byte[] data;
            private KafkaMessageSetEnumerator enumerator;

            public KafkaMessageSet(int partition, byte[] messageBytes)
            {
                if (messageBytes == null || messageBytes.Length <= 0)
                {
                    throw new ArgumentException("messageBytes is null or empty");
                }

                partitionId = partition;
                data = messageBytes;
            }

            public IEnumerator<KafkaMessage> GetEnumerator()
            {
                enumerator = new KafkaMessageSetEnumerator(this.partitionId, this.data, KafkaMessageCompressionCodec.None, false);
                return enumerator;
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
            private enum KafkaMessageSetEnumeratorState
            {
                Ready,
                NotReady,
                Done,
                Failed
            }

            private enum KafkaMessageCompressionCodec
            {
                None = 0,
                GZip = 1,
                Snappy = 2,
                LZ4 = 3
            }

            private class KafkaMessageSetEnumerator : IEnumerator<KafkaMessage>
            {
                private byte[] setBytes;
                private KafkaMessageSetEnumeratorState state;
                private KafkaMessageSetEnumerator innerEnumerator;
                private Stream stream;
                private KafkaMessageCompressionCodec codectype;
                private KafkaMessage next;
                private int partitionId;
                private KafkaBinaryReader reader;
                private long makeId = 0;
                private bool nestedzip;

                private long MakeId()
                {
                    return Interlocked.Increment(ref makeId);
                }

                public KafkaMessageSetEnumerator(int partition, byte[] data, KafkaMessageCompressionCodec compressionCodec, bool zipNested)
                {
                    this.partitionId = partition;
                    this.setBytes = data;
                    this.codectype = compressionCodec;
                    this.state = KafkaMessageSetEnumeratorState.NotReady;

                    this.nestedzip = zipNested;

                    this.init();
                }

                private void init()
                {
                    switch (codectype)
                    {
                        case KafkaMessageCompressionCodec.None:
                            stream = new MemoryStream(setBytes);
                            break;
                        case KafkaMessageCompressionCodec.GZip:
                            try
                            {
                                stream = new MemoryStream();
                                using (var ms = new MemoryStream(setBytes))
                                {
                                    using (var gzipStream = new GZipStream(ms, CompressionMode.Decompress))
                                    {
                                        //gzipStream.CopyTo(stream);
                                        int readsize = 0;
                                        int size = 4096;
                                        byte[] bytes = new byte[size];
                                        while ((readsize = gzipStream.Read(bytes, 0, size)) > 0)
                                        {
                                            stream.Write(bytes, 0, readsize);
                                        }

                                    }
                                }
                            }
                            catch (OutOfMemoryException ome)
                            {
                                try
                                {
                                    var file = Path.Combine(Path.GetTempPath(), Path.GetTempFileName());
                                    //logger.Debug("[HANDLED-EXCEPTION] OutOfMemoryException : use file as cache " + file);

                                    stream = new FileStream(file, FileMode.Create);

                                    using (var ms = new MemoryStream(setBytes))
                                    {
                                        using (var gzipStream = new GZipStream(ms, CompressionMode.Decompress))
                                        {
                                            int readsize = 0;
                                            int size = 4096;
                                            byte[] bytes = new byte[size];
                                            long total = 0;
                                            while ((readsize = gzipStream.Read(bytes, 0, size)) > 0)
                                            {
                                                stream.Write(bytes, 0, readsize);
                                                total += readsize;
                                            }

                                            //logger.Debug("GZIP => decompress from bytes " + setBytes.Length + " now total unziped " + total + " in file " + file);
                                        }
                                    }
                                }
                                catch (Exception e)
                                {
                                    //logger.Debug("[UNHANDLED-EXCEPTION]RecordEnumerator : failed to decompress gzip data " + e.StackTrace);
                                    throw e;
                                }
                            }

                            break;

                        case KafkaMessageCompressionCodec.Snappy:
                            stream = new MemoryStream(SnappyDecompress.Decompress(setBytes));
                            break;

                        default:
                            throw new UnknownCodecException("[UNHANDLED-EXCEPTION] unknown compression type");
                    }

                    stream.Seek(0, SeekOrigin.Begin);
                    reader = new KafkaBinaryReader(stream);
                }

                private bool innerDone()
                {
                    return innerEnumerator == null || !innerEnumerator.MoveNext();
                }

                public void Dispose()
                {
                    if (stream != null)
                    {
                        if (stream is FileStream)
                        {
                            var fs = (FileStream)stream;
                            if (fs != null)
                            {
                                File.Delete(fs.Name);
                                //logger.Debug("gzip cache file deleted " + fs.Name);
                            }
                        }

                        stream.Dispose();
                    }

                    if (reader != null)
                    {
                        reader.Dispose();
                    }
                }

                public bool MoveNext()
                {
                    switch (state)
                    {
                        case KafkaMessageSetEnumeratorState.Failed:
                            throw new IllegalStateException("[UNHANDLED-EXCEPTION] Iterator is in failed state");
                            break;
                        case KafkaMessageSetEnumeratorState.Done:
                            return false;
                            break;
                        case KafkaMessageSetEnumeratorState.Ready:
                            return true;
                            break;
                        default:
                            return computeNext();
                    }

                }

                private KafkaMessage allDone()
                {
                    state = KafkaMessageSetEnumeratorState.Done;
                    return null;
                }

                private bool computeNext()
                {
                    // try to get next
                    state = KafkaMessageSetEnumeratorState.Failed;
                    next = makenext();
                    if (state == KafkaMessageSetEnumeratorState.Done)
                    {
                        return false;
                    }

                    state = KafkaMessageSetEnumeratorState.Ready;
                    return true;
                }

                private KafkaMessage makenext()
                {
                    var currentMakeId = this.MakeId();

                    if (innerDone())
                    {
                        KafkaMessage decoded = null;
                        try
                        {
                            long offset = 0;
                            byte[] rec = null;
                            int size = 0;

                            long startFrom = stream.Position;

                            if (stream.Position >= stream.Length)
                            {
                                return allDone();
                            }

                            lock (reader)
                            {
                                // read from the stream
                                offset = reader.ReadInt64();
                                size = reader.ReadInt32();
                                if (size < 0)
                                    throw new IllegalStateException("[UNHANDLED-EXCEPTION] record with size " + size);

                                //rec = reader.ReadBytes(size);

                                rec = new byte[size];
                                for (int i = 0; i < size; i++)
                                {
                                    rec[i] = reader.ReadByte();
                                }
                            }

                            if (offset <= 0 || size <= 0 || rec == null || rec.Length <= 0)
                            {
                                throw new Exception("[UNHANDLED-EXCEPTION] failed to get data");
                            }

                            byte attribute = 0;
                            byte[] key = null;
                            int payloadlength = 0;
                            byte[] payload = null;
                            using (var ms = new MemoryStream(rec))
                            {
                                using (var rd = new KafkaBinaryReader(ms))
                                {
                                    var crc = rd.ReadInt32(); //4 crc
                                    var magic = rd.ReadByte(); //1 magic
                                    attribute = rd.ReadByte(); // 1 attribute
                                    var keylength = rd.ReadInt32();

                                    if (keylength != -1)
                                    {
                                        key = rd.ReadBytes(keylength);
                                    }

                                    payloadlength = rd.ReadInt32();
                                    if (payloadlength == 0)
                                    {
                                        return null;
                                    }

                                    payload = rd.ReadBytes(payloadlength);

                                    //logger.Debug("partition id " + this.partitionId + " make id " + currentMakeId + " magic " + magic + " codec " + codec + " key size " + keylength + " value size " + payloadlength + " stream begin " + startFrom + " now " + stream.Position + " message packet " + (8 + 4 + size) + " message body " + size + " offset " + offset);

                                }
                            }

                            var codec = (KafkaMessageCompressionCodec)(attribute & 3);
                            if (codec == KafkaMessageCompressionCodec.None || nestedzip)
                            {
                                decoded = new KafkaMessage(payload, key, CompressionCodecsHelper.GetCompressionCodec(attribute & 3))
                                {
                                    Offset = offset,
                                    PartitionId = this.partitionId
                                };

                            }
                            else
                            {
                                // element format in record set
                                //   8bytes    4bytes  size bytes
                                // +---------+------+---------+
                                // | offset  | size | message |
                                // +---------+------+---------+

                                // message format
                                //  4bytes 1byte   1byte  4+?      4+? 
                                // +------+-------+------+-------+--------+  
                                // | crc  | magic | att  | key   | value  |
                                // +------+-------+------+-------+--------+  
                                // 
                                // this message is zipped, only the real value of the message (payload) itself is considered as zipped data
                                var data = new byte[payloadlength];
                                Array.Copy(payload, 0, data, 0, data.Length);

                                // init the inner iterator with the value payload of the message,
                                // which will de-compress the payload to a set of messages;
                                // since we assume nested compression is not allowed, the deep iterator
                                // would not try to further decompress underlying messages
                                innerEnumerator = new KafkaMessageSetEnumerator(this.partitionId, data, codec, true);
                                decoded = innerEnumerator.Current;
                            }

                        }
                        catch (EndOfStreamException)
                        {
                            state = KafkaMessageSetEnumeratorState.Done;
                            //logger.Debug("[HANDLED-EXCEPTION] makenext => end of stream, so all messages are decoded.");
                        }
                        catch (Exception e)
                        {
                            //logger.Debug("[UNHANDLED-EXCEPTION] makenext => failed to decode data. " + e.Message + " " + e.StackTrace);
                            throw;
                        }
                        return decoded;
                    }
                    else
                    {
                        return innerEnumerator.Current;
                    }
                }

                public void Reset()
                {
                    if (stream != null)
                    {
                        stream.Seek(0, SeekOrigin.Begin);
                    }
                }

                public KafkaMessage Current
                {
                    get
                    {
                        if (!MoveNext())
                            throw new Exception("[UNHANDLED-EXCEPTION] no such element");
                        state = KafkaMessageSetEnumeratorState.NotReady;
                        if (next == null)
                            throw new IllegalStateException("[UNHANDLED-EXCEPTION] expected item but none found.");

                        return next;
                    }
                }

                object IEnumerator.Current
                {
                    get { return Current; }
                }
            }

            public void Dispose()
            {
                if (enumerator != null)
                {
                    enumerator.Dispose();
                }
            }
        }

        private class KafkaRequestResponse
        {
            public long ReqTaskId { get; set; }
            public int Broker { get; set; }
            public List<KafkaFetchRequestEntry> Requests { get; set; }
            public FetchResponse Response { get; set; }
        }


        void updateFetchEntities(Dictionary<string, Dictionary<int, long>> userOffsets)
        {
            foreach (var kvp in userOffsets)
            {
                var topic = kvp.Key;
                var offsets = kvp.Value;

                foreach (var offset in offsets)
                {
                    var partition = offset.Key;
                    var position = offset.Value;

                    var targetMetadata =
                        brokerOrderedMetadatas.First(
                            x =>
                                x.Value.ContainsKey(topic) &&
                                x.Value[topic].Exists(e => e.PartitionId == partition));
                    var leaderId = targetMetadata.Key;

                    if (!brokerOrderedFetchRequestEntries.ContainsKey(leaderId))
                    {
                        brokerOrderedFetchRequestEntries.TryAdd(leaderId, new List<KafkaFetchRequestEntry>());
                    }

                    if (brokerOrderedFetchRequestEntries[leaderId].Exists(
                            x => x.Topic == topic && x.PartitionId == partition))
                    {
                        var entry =
                            brokerOrderedFetchRequestEntries[leaderId].First(
                                x => x.Topic == topic && x.PartitionId == partition);

                        if (position != -1)
                        {
                            entry.Position = position;
                        }
                    }
                    else
                    {
                        brokerOrderedFetchRequestEntries[leaderId].Add(new KafkaFetchRequestEntry()
                        {
                            BrokerId = leaderId,
                            Topic = topic,
                            PartitionId = partition,
                            Position = -1
                        });
                    }
                }

            }
        }

        int ensureCoordinator()
        {
            if (!coordinatorReadyEvent.WaitOne(TimeSpan.FromMinutes(2)))
            {
                var id = findCoordinatorFunc();
                lock (refreshLock)
                {
                    coordinatorId = id;
                }

                coordinatorReadyEvent.Set();
            }

            return coordinatorId;
        }

        int findCoordinatorFunc()
        {
            var coordinatorRequest = new GroupCoordinatorRequest(this.config.GroupId, 0, 0, this.config.ConsumerId);

            DateTime startUtc = DateTime.UtcNow;
            // send to any broker
            var bk = sockets.First();
            var bid = bk.Key;
            var socket = bk.Value;
            GroupCoordinatorResponse response = null;

            if (!this.useAsyncSocket)
            {
                logger.Debug("findCoordinatorFunc => lock on socket wait for " + bid);
                lock (socket)
                {
                    logger.Debug("findCoordinatorFunc => lock on socket acquired " + bid + " in " + TimeSpan.FromTicks(DateTime.UtcNow.Ticks - startUtc.Ticks).TotalSeconds + " seconds");
                    if (!socket.Connected)
                    {
                        socket.Connect(socket.RemoteEndPoint);
                    }

                    var stream = new NetworkStream(socket)
                    {
                        ReadTimeout = this.config.ReceiveTimeout,
                        WriteTimeout = this.config.SendTimeout
                    };
                    var byteSends = coordinatorRequest.Serialize();
                    stream.Write(byteSends, 0, byteSends.Length);
                    var reader = new KafkaBinaryReader(stream);
                    response = GroupCoordinatorResponse.ParseFrom(reader);
                }
            }
            else
            {
                var asyncSocket = asyncSockets[bid];
                response = asyncSocket.Send(coordinatorRequest);
            }

            if (response == null)
            {
                throw new Exception("[UNHANDLED-EXCEPTION] findCoordinatorFunc => null GroupCoordinatorResponse");
            }

            int id = -1;
            if (response.ErrorCode == 0)
            {
                id = response.CoordinatorId;
            }

            logger.Debug("findCoordinatorFunc => coordinator response from broker " + bid + " in " +
                         TimeSpan.FromTicks(DateTime.UtcNow.Ticks - startUtc.Ticks).TotalSeconds +
                         " seconds coordinator broker " + id);

            return id;

        }

        void commitOffsetAction()
        {
            var requestInfo = new Dictionary<string, List<PartitionOffsetCommitRequestInfo>>();

            foreach (var kvp in brokerOrderedFetchRequestEntries)
            {
                var bid = kvp.Key;
                var entities = kvp.Value;

                foreach (var e in entities)
                {
                    // check whether current offset is the same as last commited
                    if (this._lastAutoCommittedOffset.ContainsKey(e.Topic)
                        && this._lastAutoCommittedOffset[e.Topic].ContainsKey(e.PartitionId)
                        && this._lastAutoCommittedOffset[e.Topic][e.PartitionId] == e.Position)
                    {
                        //logger.Debug("commitOffsetAction => offset for partition " + e.PartitionId + " is " + e.Position + " has not changed since last commit. last " + this._lastAutoCommittedOffset[e.Topic][e.PartitionId] + " now " + e.Position);
                        continue;
                    }

                    if (e.Position != -1)
                    {
                        if (!requestInfo.ContainsKey(e.Topic))
                        {
                            requestInfo.Add(e.Topic, new List<PartitionOffsetCommitRequestInfo>());
                        }

                        requestInfo[e.Topic].Add(new PartitionOffsetCommitRequestInfo(e.PartitionId, e.Position, ""));
                    }
                }
            }

            if (requestInfo.Count <= 0)
            {
                logger.Debug("commitOffsetAction => no partitions with new offsets to commit, so skip now");
                return;
            }

            DateTime startUtc = DateTime.UtcNow;

            // commit the batch

            int generationId;
            string memberId;

            // !!! important !!!
            //Note that when this API is used for a "simple consumer," which is not part of a consumer group, then the generationId must be set to -1 and the memberId must be empty (not null). 
            if (!this.config.BalanceInGroupMode)
            {
                generationId = -1;
                memberId = "";
            }
            else
            {
                generationId = this.generationIdInGroup;
                memberId = this.memberIdInGroup;
            }

            OffsetCommitRequest offsetCommitRequest = new OffsetCommitRequest(this.config.GroupId, generationId, memberId, -1, requestInfo);
            var bytesSend = offsetCommitRequest.Serialize();
            var coordinator = ensureCoordinator();

            var socket = sockets[coordinator];
            OffsetCommitResponse response = null;

            startUtc = DateTime.UtcNow;
            if (!this.useAsyncSocket)
            {
                logger.Debug("commitOffsetAction => lock on socket wait for " + coordinator);
                lock (socket)
                {
                    logger.Debug("commitOffsetAction => lock on socket acquired " + coordinator + " in " + TimeSpan.FromTicks(DateTime.UtcNow.Ticks - startUtc.Ticks).TotalSeconds + " seconds");
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
                    response = OffsetCommitResponse.ParseFrom(reader);
                }
            }
            else
            {
                var asyncSocket = this.asyncSockets[coordinator];
                response = asyncSocket.Send(offsetCommitRequest);
            }

            if (response == null)
            {
                throw new Exception("[UNHANDLED-EXCEPTION] commitOffsetAction => null OffsetCommitResponse");
            }

            logger.Debug("commitOffsetAction => commit offset response from broker " + coordinator + " in " +
                         TimeSpan.FromTicks(DateTime.UtcNow.Ticks - startUtc.Ticks).TotalSeconds +
                         " seconds coordinator broker " + coordinator);

            foreach (var info in response.ResponseInfo)
            {
                var tp = info.Key;
                var partitionInfos = info.Value;
                foreach (var pinfo in partitionInfos)
                {
                    if (pinfo.ErrorCode == 0)
                    {
                        long offset = -1;
                        if (requestInfo.ContainsKey(tp))
                        {
                            var targetoffset =
                                requestInfo[tp].FirstOrDefault(e => e.PartitionId == pinfo.PartitionId);
                            if (targetoffset != null)
                            {
                                offset = targetoffset.Offset;
                                if (offset != -1)
                                {
                                    // update last commited offset cache
                                    if (!this._lastAutoCommittedOffset.ContainsKey(tp))
                                    {
                                        this._lastAutoCommittedOffset.Add(tp, new Dictionary<int, long>());
                                    }

                                    if (offset != 0)
                                    {
                                        if (!this._lastAutoCommittedOffset[tp].ContainsKey(pinfo.PartitionId))
                                        {
                                            this._lastAutoCommittedOffset[tp].Add(pinfo.PartitionId, offset);
                                        }
                                        else
                                        {
                                            this._lastAutoCommittedOffset[tp][pinfo.PartitionId] = offset;
                                        }

                                        logger.Debug("commitOffsetAction => offset for partition " +
                                                     pinfo.PartitionId + " is " + offset + " commit succeeded , errorCode " +
                                                     pinfo.ErrorCode);
                                    }
                                }
                                else
                                {
                                    logger.Debug("commitOffsetAction => offset for partition " + pinfo.PartitionId + " offset " + offset +
                                                 " commit succeeded but returns offset -1 , errorCode " + pinfo.ErrorCode);
                                }

                            }
                            else
                            {
                                logger.Debug(
                                    "commitOffsetAction => offset for partition " + pinfo.PartitionId + " offset " + offset + " commit succeeded but response does not contains info, errorCode " +
                                    pinfo.ErrorCode);
                            }

                        }
                        else
                        {
                            logger.Debug(
                                string.Format("commitOffsetAction => offset commit succeeded but response does not contains topic " +
                                              tp));
                        }

                    }
                    else
                    {
                        logger.Debug("commitOffsetAction => commitOffset: offset commit failed with error " +
                                     pinfo.ErrorCode);

                        if (pinfo.ErrorCode == 25) // unknow member id
                        {
                            // need to re-assign partitions
                        }

                        if (pinfo.ErrorCode == 22) // illegal generation
                        {
                        }

                        if (pinfo.ErrorCode == 27) // rebalance in progress
                        {
                        }

                    }
                }
            }

        }

        void syncGroupAction()
        {
            var syncInfo = new List<SyncGroupRequestGroupAssignmentInfo>();
            foreach (var kvp in groupSyncRequestMap)
            {
                var _id = kvp.Key;
                var _assignment = new SyncGroupResponseMemberAssignmentInfo();
                _assignment.Version = 0;
                _assignment.UserData = null;

                // group by topic [<topic,partitionid>]
                Dictionary<string, List<int>> _cache = new Dictionary<string, List<int>>();
                foreach (var p in kvp.Value)
                {
                    if (!_cache.ContainsKey(p.Key))
                    {
                        _cache.Add(p.Key, new List<int>());
                    }

                    _cache[p.Key].Add(p.Value);
                }

                _assignment.PartitionAssignmentInfos = _cache.Select(g => new SyncGroupResponsePartitionAssignmentInfo() { Topic = g.Key, Partitions = g.Value.ToArray() }).ToArray();

                syncInfo.Add(new SyncGroupRequestGroupAssignmentInfo(_id, _assignment.Serialize()));
            }

            var syncGroupRequest = new SyncGroupRequest(this.config.GroupId, generationIdInGroup, memberIdInGroup, syncInfo) { ClientId = this.config.ConsumerId };
            var bytesSend = syncGroupRequest.Serialize();
            var coordinator = ensureCoordinator();

            var socket = sockets[coordinator];
            SyncGroupResponse response = null;
            DateTime startUtc = DateTime.UtcNow;

            if (!this.useAsyncSocket)
            {
                logger.Debug("syncGroupAction => lock on socket wait for " + coordinator);
                lock (socket)
                {
                    logger.Debug("syncGroupAction => lock on socket acquired " + coordinator + " in " + TimeSpan.FromTicks(DateTime.UtcNow.Ticks - startUtc.Ticks).TotalSeconds + " seconds");
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
                    response = SyncGroupResponse.ParseFrom(reader);
                }
            }
            else
            {
                var asyncSocket = this.asyncSockets[coordinator];
                response = asyncSocket.Send(syncGroupRequest);
            }


            if (response == null)
            {
                throw new Exception("[UNHANDLED-EXCEPTION] syncGroupAction => null SyncGroupResponse");
            }

            var errorCode = response.ErrorCode;
            if (errorCode == 0)
            {
                var assignment = response.ParseMemberAssignment();

                var ids = string.Join(",", assignment.PartitionAssignmentInfos.Select(x => "[" + x.Topic + ":" + string.Join(",", x.Partitions) + "]"));

                // update the fetch maps
                foreach (var pai in assignment.PartitionAssignmentInfos)
                {
                    var tp = pai.Topic;
                    var pids = pai.Partitions;

                    // initialize the fetch map from sync results
                    foreach (var pid in pids)
                    {
                        var targetMetadata = brokerOrderedMetadatas.First(x => x.Value.ContainsKey(tp) && x.Value[tp].Exists(y => y.PartitionId == pid));
                        var bid = targetMetadata.Key;

                        if (!brokerOrderedFetchRequestEntries.ContainsKey(bid))
                        {
                            brokerOrderedFetchRequestEntries.TryAdd(bid, new List<KafkaFetchRequestEntry>());
                        }

                        if (!brokerOrderedFetchRequestEntries[bid].Exists(
                            x => x.BrokerId == bid && x.Topic == tp && x.PartitionId == pid))
                        {
                            brokerOrderedFetchRequestEntries[bid].Add(new KafkaFetchRequestEntry()
                            {
                                BrokerId = bid,
                                Topic = tp,
                                PartitionId = pid,
                                Position = -1
                            });
                        }
                    }
                }

                needRebalance = false;
                logger.Debug("syncGroupAction => needRebalance is now " + needRebalance + " because sync group completed ");
                logger.Debug("syncGroupAction => sync group completed with assignments " + ids + " for consumer " + this.config.ConsumerId);
            }
            else
            {
                logger.Debug("syncGroupAction => sync group get error " + errorCode);
                if (errorCode == 25)
                {
                    // need to re-join group
                    memberIdInGroup = "";
                    needRebalance = true;
                    logger.Debug("syncGroupAction => needRebalance is now " + needRebalance + " because sync group get error " + errorCode);
                }
                else if (errorCode == 27)
                {
                    // a rebalance in progress
                }
            }
        }

        void joinGroupAsLeaderAction(JoinGroupResponse response)
        {
            // now try to sync between members, because we are the leader
            var members = response.Members;

            // [memberid => [topic]]
            var memberToTopicsMap = new Dictionary<string, List<string>>();
            foreach (var member in members)
            {
                var id = member.MemberId;
                var metadata = member.ParseMemberMetadata();
                if (!memberToTopicsMap.ContainsKey(id))
                {
                    memberToTopicsMap.Add(id, new List<string>());
                }

                memberToTopicsMap[id].AddRange(metadata.Topics);

            }

            // [topic => [memberid]]
            var topicToMemberIdsMap = new Dictionary<string, List<String>>();
            foreach (var kvp in memberToTopicsMap)
            {
                var _id = kvp.Key;
                var _topics = kvp.Value;
                foreach (var tp in _topics)
                {
                    if (!topicToMemberIdsMap.ContainsKey(tp))
                    {
                        topicToMemberIdsMap.Add(tp, new List<string>());
                    }

                    topicToMemberIdsMap[tp].Add(_id);
                }
            }

            // [topic => [partitions]]
            var topicToPartitionsMap = new Dictionary<string, List<int>>();
            foreach (var kvp in topicToMemberIdsMap)
            {
                var topic = kvp.Key;
                var partitionInfo = new List<TopicMetadataResponsePartitionInfo>();
                partitionInfo = brokerOrderedMetadatas.SelectMany(e => e.Value.Where(v => v.Key == topic))
                    .SelectMany(t => t.Value).ToList();



                logger.Debug(
                    string.Format("joinGroupAsLeaderAction => cached metadata topic {0} partitions {1}", topic,
                        string.Join(",", partitionInfo.Select(p => p.PartitionId).OrderBy(t => t))));

                if (!topicToPartitionsMap.ContainsKey(topic))
                {
                    topicToPartitionsMap.Add(topic, new List<int>());
                }

                if (partitionInfo.Any())
                {
                    var pids = partitionInfo.Select(p => p.PartitionId).ToList();
                    foreach (var pid in pids)
                    {
                        if (!topicToPartitionsMap[topic].Contains(pid))
                        {
                            topicToPartitionsMap[topic].Add(pid);
                        }
                    }
                }
            }

            // [memberid => [topic => partition]]
            // ********************************************************//
            // core of assign partitions of same topic bewteen different 
            // consumers in the same gorup
            // ********************************************************//
            foreach (var kvp in topicToMemberIdsMap)
            {
                var topic = kvp.Key;
                var groupmembers = kvp.Value;
                if (!topicToPartitionsMap.ContainsKey(topic))
                {
                    continue;
                }

                var _partitions = topicToPartitionsMap[topic].OrderBy(t => t).ToArray();

                var partitionToConsumerRatio = _partitions.Length / groupmembers.Count;
                var extraRatio = _partitions.Length % groupmembers.Count;

                for (int i = 0; i < groupmembers.Count; i++)
                {
                    var _begin = partitionToConsumerRatio * i + Math.Min(i, extraRatio);
                    var _size = partitionToConsumerRatio + (i + 1 > extraRatio ? 0 : 1);
                    var _partitionsToOwn = new int[_size];
                    Array.Copy(_partitions, _begin, _partitionsToOwn, 0, _size);

                    logger.Debug(string.Format("joinGroupAsLeaderAction => consumer {0} should get range {1} - {2} in partitions {3}", groupmembers[i], _begin, _begin + _size, string.Join(",", _partitionsToOwn)));

                    // initialize the fetch request cache

                    if (!groupSyncRequestMap.ContainsKey(groupmembers[i]))
                    {
                        groupSyncRequestMap.Add(groupmembers[i], new List<KeyValuePair<string, int>>());
                    }

                    // clear old assignment before add new ones, otherwise will get more than what need to own
                    groupSyncRequestMap[groupmembers[i]].Clear();
                    groupSyncRequestMap[groupmembers[i]].AddRange(_partitionsToOwn.Select(p => new KeyValuePair<string, int>(topic, p)));

                    logger.Debug(string.Format("joinGroupAsLeaderAction => consumer {0} assigned with topic {1} partitions {2}", groupmembers[i], topic, string.Join(",", groupSyncRequestMap[groupmembers[i]].Select(a => a.Value).OrderBy(t => t))));
                }
            }

            // now sync with other members
            syncGroupAction();

        }

        void joinGroupAsFollowerAction()
        {
            // if not leader in group, then just ask for assignment
            groupSyncRequestMap = new Dictionary<string, List<KeyValuePair<string, int>>>();

            // sync with group
            syncGroupAction();
        }

        void joinGroupAction()
        {
            short version = 0;

            byte[] userData = null;
            var protocolMetadata = new JoinGroupRequestProtocolMetadataInfo(version, this._subscriptions, userData);
            var metadataBytes = protocolMetadata.Serialize();


            var timtout = 30000;
            var protocolType = KafkaConsumerGroupProtocol.ProtocolType;
            var protocols = new JoinGroupRequestProtocolInfo[]
            {
                new JoinGroupRequestProtocolInfo(KafkaConsumerGroupProtocol.ProtocolName, metadataBytes)
            };

            var joinRequest = new JoinGroupRequest(this.config.ConsumerId, this.config.GroupId, timtout, memberIdInGroup, protocolType, protocols);
            var sendBytes = joinRequest.Serialize();

            var coordinator = ensureCoordinator();

            var socket = sockets[coordinator];
            JoinGroupResponse joinResponse = null;
            DateTime startUtc = DateTime.UtcNow;

            if (!this.useAsyncSocket)
            {
                logger.Debug("joinGroupAction => lock on socket wait for " + coordinator);
                lock (socket)
                {
                    logger.Debug("joinGroupAction => lock on socket acquired " + coordinator + " in " + TimeSpan.FromTicks(DateTime.UtcNow.Ticks - startUtc.Ticks).TotalSeconds + " seconds");
                    if (!socket.Connected)
                    {
                        socket.Connect(socket.RemoteEndPoint);
                    }

                    var stream = new NetworkStream(socket)
                    {
                        ReadTimeout = this.config.ReceiveTimeout,
                        WriteTimeout = this.config.SendTimeout
                    };
                    stream.Write(sendBytes, 0, sendBytes.Length);

                    var reader = new KafkaBinaryReader(stream);
                    joinResponse = JoinGroupResponse.ParseFrom(reader);
                }
            }
            else
            {
                var asyncSocket = this.asyncSockets[coordinator];
                joinResponse = asyncSocket.Send(joinRequest);
            }

            if (joinResponse == null)
            {
                throw new Exception("[UNHANDLED-EXCEPTION] joinGroupAction => null JoinGroupResponse");
            }

            short errorCode = joinResponse.ErrorCode;
            if (errorCode == 0)
            {
                memberIdInGroup = joinResponse.MemberId;
                generationIdInGroup = joinResponse.GenerationId;
                var leaderInGroup = joinResponse.LeaderId;
                var members = joinResponse.Members;

                if (memberIdInGroup == leaderInGroup)
                {
                    logger.Debug("joinGroupAction => " + this.config.ConsumerId + " join group as leader");
                    joinGroupAsLeaderAction(joinResponse);
                }
                else
                {
                    logger.Debug("joinGroupAction => " + this.config.ConsumerId + " join group as follower");
                    joinGroupAsFollowerAction();
                }

                logger.Debug("joinGroupAction => join group completed, now start heartbeating");
                this.rebalanceComplete = true;
            }
            else
            {
                logger.Debug("joinGroupAction => join group failed with error " + errorCode);
                if (errorCode == 25)
                {
                    memberIdInGroup = "";
                    needRebalance = true;
                    logger.Debug("joinGroupAction => needRebalance is now " + needRebalance + " because join group failed with error " + errorCode);
                }
            }
        }

        void heartbeatAction()
        {
            var memberId = this.memberIdInGroup;
            var groupId = this.config.GroupId;
            var generation = generationIdInGroup;
            logger.Debug("heartbeatAction => try to find coordinator for heartbeat");
            var coordinator = ensureCoordinator();

            logger.Debug("heartbeatAction => coordinator found for heartbeat");

            var heartbeatRequest = new HeartbeatRequest(groupId, generation, memberId) { ClientId = this.config.ConsumerId };
            var bytesSend = heartbeatRequest.Serialize();
            var socket = this.sockets[coordinator];
            HeartbeatResponse heartbeatResponse = null;
            DateTime startUtc = DateTime.UtcNow;

            if (!this.useAsyncSocket)
            {
                logger.Debug("heartbeatAction => lock on socket wait for " + coordinator);
                lock (socket)
                {
                    logger.Debug("heartbeatAction => lock on socket acquired " + coordinator + " in " + TimeSpan.FromTicks(DateTime.UtcNow.Ticks - startUtc.Ticks).TotalSeconds + " seconds");
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
                    heartbeatResponse = HeartbeatResponse.ParseFrom(reader);
                }
            }
            else
            {
                var asyncSocket = this.asyncSockets[coordinator];
                logger.Debug("heartbeatAction => use asyncSocket to send hearbeat");
                heartbeatResponse = asyncSocket.Send(heartbeatRequest);
            }

            if (heartbeatResponse == null)
            {
                throw new Exception("[UNHANDLED-EXCEPTION] heartbeatAction => null HeartbeatResponse");
            }
            if (heartbeatResponse.ErrorCode == 0)
            {
                // no error found
                logger.Debug("heartbeatAction => hearbeat succeeded");
            }
            else
            {
                logger.Debug("heartbeatAction => hearbeat failed");
                logger.Debug(string.Format("heartbeatAction => consumer {0} heart beat get error: {1}", this.config.ConsumerId, heartbeatResponse.ErrorCode));

                if (heartbeatResponse.ErrorCode == 15)
                {
                    // group coordinator is not available
                }
                else if (heartbeatResponse.ErrorCode == 16)
                {
                    // the coordinator is not real coordinator for this group
                }
                else if (heartbeatResponse.ErrorCode == 27)
                {
                    //REBALANCE_IN_PROGRESS
                    // !!!! this is how we detect when to do re-balance !!!
                    // the heartbeat will detect the changes in group when there are new members join in the group
                    // a reblance is in progress
                    //TODO: need to re-join the group to get assignment
                    logger.Debug(string.Format("heartbeatAction => consumer {0} heart beat failed because rebalance in progress (error 27), try re-join by invoking JoinGroup", this.config.ConsumerId));
                    this.needRebalance = true;
                    logger.Debug("heartbeatAction => needRebalance is now " + needRebalance + " because rebalance in progress " + heartbeatResponse.ErrorCode);
                }
                else if (heartbeatResponse.ErrorCode == 22)
                {
                    // the generation is illegal
                }
                else if (heartbeatResponse.ErrorCode == 25)
                {
                    // the consumer id is unknown, need to re-join the group
                    logger.Debug(string.Format("heartbeatAction => consumer {0} heart beat failed because unknown consumer id (error 25), try re-join by invoking JoinGroup", this.config.ConsumerId));
                    this.needRebalance = true;
                    logger.Debug("heartbeatAction => needRebalance is now " + needRebalance + " because consumer id is unknown " + heartbeatResponse.ErrorCode);
                }
                else
                {
                    // other scenarios
                }
            }

        }

        Dictionary<string, Dictionary<int, long>> consumerGroupOffsetFunc(string topic, string groupId)
        {
            var groupOffsets = new Dictionary<string, Dictionary<int, long>>();
            /*
            if (!this.brokerOrderedMetadatas.Any())
            {
                this.metadataAction();
            }
            */
            if (!metadataReadyEvent.WaitOne(TimeSpan.FromMinutes(5)))
            {
                this.metadataAction();
                metadataReadyEvent.Set();
            }
            foreach (var kvp in this.brokerOrderedMetadatas)
            {
                var bid = kvp.Key;
                var metadatas = kvp.Value;

                if (!metadatas.ContainsKey(topic))
                {
                    continue;
                }

                var pinfo = metadatas[topic];
                var requestInfo = new Dictionary<string, List<int>>();
                requestInfo.Add(topic, pinfo.Select(pi => pi.PartitionId).ToList());
                var offsetRequest = new OffsetFetchRequest(groupId, requestInfo);
                var bytesSend = offsetRequest.Serialize();

                var coordinator = ensureCoordinator();

                var socket = this.sockets[coordinator];
                OffsetFetchResponse offsetFetchResponse = null;
                DateTime startUtc = DateTime.UtcNow;

                if (!this.useAsyncSocket)
                {
                    logger.Debug("consumerGroupOffsetFunc => lock on socket wait for " + coordinator);
                    lock (socket)
                    {
                        logger.Debug("consumerGroupOffsetFunc => lock on socket acquired " + bid + " in " + TimeSpan.FromTicks(DateTime.UtcNow.Ticks - startUtc.Ticks).TotalSeconds + " seconds");
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
                        offsetFetchResponse = OffsetFetchResponse.ParseFrom(reader);
                    }
                }
                else
                {
                    var asyncSocket = this.asyncSockets[coordinator];
                    offsetFetchResponse = asyncSocket.Send(offsetRequest);
                }

                if (offsetFetchResponse == null)
                {
                    throw new Exception("[UNHANDLED-EXCEPTION] null offsetFetchResponse");
                }

                if (!offsetFetchResponse.ResponseInfo.ContainsKey(topic))
                {
                    return groupOffsets;
                }

                var targetOffsetInfo = offsetFetchResponse.ResponseInfo[topic];
                foreach (var info in targetOffsetInfo)
                {
                    var pid = info.Partition;
                    long off;
                    if (info.ErrorCode == 0)
                    {
                        off = info.Offset;
                    }
                    else
                    {
                        // read the real data offset as its current offset
                        off = -1;
                    }

                    if (!groupOffsets.ContainsKey(topic))
                    {
                        groupOffsets.Add(topic, new Dictionary<int, long>());
                    }

                    if (!groupOffsets[topic].ContainsKey(pid))
                    {
                        groupOffsets[topic].Add(pid, off);
                    }
                    else
                    {
                        groupOffsets[topic][pid] = off;
                    }
                }
            }

            return groupOffsets;
        }

        void ensureFetchOffsets()
        {
            if (this.fetchOffsetAssigned)
            {
                return;
            }

            // give all partitions to this consumer
            Dictionary<string, Dictionary<int, long>> assignments = new Dictionary<string, Dictionary<int, long>>();
            foreach (var topic in this._subscriptions)
            {
                var all = this.brokerOrderedMetadatas.SelectMany(x => x.Value.Where(y => y.Key == topic)).SelectMany(x => x.Value);
                if (!assignments.ContainsKey(topic))
                {
                    assignments.Add(topic, new Dictionary<int, long>());
                }

                foreach (var pm in all)
                {
                    if (!assignments[topic].ContainsKey(pm.PartitionId))
                    {
                        // if the offset exist
                        var fetchEntities = this.brokerOrderedFetchRequestEntries.SelectMany(x => x.Value);
                        var target = fetchEntities.FirstOrDefault(x => x.Topic == topic && x.PartitionId == pm.PartitionId);
                        assignments[topic].Add(pm.PartitionId, target == null ? -1 : target.Position);
                    }
                }

                //  if the initial position is -1, then try to get committed offset
                if (assignments.Any(a => a.Value.Any(e => e.Value == -1)))
                {
                    try
                    {
                        // first try to get committed
                        var comitted = this.consumerGroupOffsetFunc(topic, this.config.GroupId);
                        foreach (var kvp in comitted)
                        {
                            if (assignments.ContainsKey(kvp.Key))
                            {
                                foreach (var o in kvp.Value)
                                {
                                    if (assignments[kvp.Key].ContainsKey(o.Key))
                                    {
                                        assignments[kvp.Key][o.Key] = o.Value;
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        logger.Debug("[UNHANDLED-EXCEPTION] fail to get committed offset for topic " + topic + " for gorup " + this.config.GroupId + " " + e.Message);
                    }
                }
            }

            // if no committed offsets, then try to get earliest or lastest
            // -1 : no fetch offset set
            // 0  : fetch offset has error
            if (assignments.Any(a => a.Value.Any(e => e.Value == -1)) || assignments.Any(a => a.Value.Any(e => e.Value == 0)))
            {
                // if no committed, try get earliest 
                if (this.config.AutoOffsetReset == OffsetRequest.SmallestTime)
                {
                    // if caller want to use the earliest offset
                    assignments = dataOffsetFunc(-2);
                }
                else
                {
                    // by default , the fetch postion should be the latest offset
                    assignments = dataOffsetFunc(-1);
                }
            }

            this.updateFetchEntities(assignments);

            this.fetchOffsetAssigned = true;
        }

        void ensureFetchEntitiesAssigned()
        {
            if (this.config.BalanceInGroupMode)
            {
                // ensure group is active
                if (this.needRebalance)
                {
                    // when rebalance happen, first save offsets
                    logger.Debug("ensureFetchEntitiesAssigned: rebalance is needed so first commit offsets.");
                    this.commitOffsetAction();

                    logger.Debug("ensureFetchEntitiesAssigned: start rebalance by join group.");
                    this.joinGroupAction();
                }
            }

            if (this.fetchEntitiesAssigned)
            {
                return;
            }

            this.fetchEntitiesAssigned = true;
        }

        void autoCommitOffsetsThread()
        {
            if (this.config.AutoCommitInterval <= 0)
            {
                this.config.AutoCommitInterval = KafkaConfiguration.DefaultAutoCommitInterval;
            }

            logger.Debug("autoCommitOffsetsThread => start auto commit offsets thread with interval " + this.config.AutoCommitInterval + " milliseconds");

            while (!this.stopRunning)
            {
                try
                {
                    this.CommitOffsets();
                }
                catch (Exception e)
                {
                    logger.Debug("[UNHANDLED-EXCEPTION] autoCommitOffsetsThread => failed \r\n" + e.Message);
                }

                Thread.Sleep(this.config.AutoCommitInterval);
            }
        }

        void heartbeatThread()
        {
            // the default interval milliseconds to heart beat
            int interval = 3 * 1000;

            while (!this.stopRunning)
            {
                try
                {
                    if (needRebalance)
                    {
                        continue;
                    }

                    if (!rebalanceComplete)
                    {
                        continue;
                    }

                    //logger.Debug("heartbeatThread => start heartbeatAction as rebalance completed at " + DateTime.UtcNow);

                    this.heartbeatAction();
                }
                catch (Exception e)
                {
                    logger.Debug("[UNHANDLED-EXCEPTION] heartbeatThread => heartbeatAction failed \r\n" + e.Message);
                }

                Thread.Sleep(interval);
            }
        }

        public void Subscribe(IEnumerable<string> topics)
        {
            foreach (var topic in topics)
            {
                if (!this._subscriptions.Contains(topic))
                {
                    this._subscriptions.Add(topic);
                }
            }
        }

        public void CommitOffsets()
        {
            if (this.config.DotNotCommitOffset)
            {
                return;
            }

            this.commitOffsetAction();
        }

        public void CommitOffset(string topic, int partition, long offset, bool setPosition = true, bool ignoreAutoCommit = false)
        {
            if (this.config.DotNotCommitOffset)
            {
                return;
            }

            if (this.config.AutoCommit == true)
            {
                if (ignoreAutoCommit == false)
                {
                    throw new ArgumentException(string.Format("When do commit offset with desired partition and offset, must set AutoCommit of ConsumerConfiguration as false!"));
                }
            }

            // check the requested topic and partition exist
            if (!brokerOrderedFetchRequestEntries.Any(x => x.Value.Exists(e => e.Topic == topic && e.PartitionId == partition)))
            {
                return;
            }

            // default retention time is -1
            var requestInfo = new Dictionary<string, List<PartitionOffsetCommitRequestInfo>>();
            requestInfo.Add(topic, new List<PartitionOffsetCommitRequestInfo>() { new PartitionOffsetCommitRequestInfo(partition, offset, "") });
            OffsetCommitRequest offsetCommitRequest;
            int generationId;
            string memberId;

            // !!! important !!!
            //Note that when this API is used for a "simple consumer," which is not part of a consumer group, then the generationId must be set to -1 and the memberId must be empty (not null). 
            if (!this.config.BalanceInGroupMode)
            {
                generationId = -1;
                memberId = "";

            }
            else
            {
                generationId = this.generationIdInGroup;
                memberId = this.memberIdInGroup;

            }

            // offset commit need to be sent to coordinator in group mode

            var coordinator = ensureCoordinator();

            var socket = this.sockets[coordinator];
            OffsetCommitResponse offsetCommitResponse = null;
            offsetCommitRequest = new OffsetCommitRequest(this.config.GroupId, generationId, memberId, -1, requestInfo);
            DateTime startUtc = DateTime.UtcNow;
            if (!this.useAsyncSocket)
            {
                logger.Debug("CommitOffset => lock on socket wait for " + coordinator);
                lock (socket)
                {
                    logger.Debug("CommitOffset => lock on socket acquired " + coordinator + " in " + TimeSpan.FromTicks(DateTime.UtcNow.Ticks - startUtc.Ticks).TotalSeconds + " seconds");
                    if (!socket.Connected)
                    {
                        socket.Connect(socket.RemoteEndPoint);
                    }
                    var bytesSend = offsetCommitRequest.Serialize();
                    var stream = new NetworkStream(socket)
                    {
                        ReadTimeout = this.config.ReceiveTimeout,
                        WriteTimeout = this.config.SendTimeout
                    };
                    stream.Write(bytesSend, 0, bytesSend.Length);

                    var reader = new KafkaBinaryReader(stream);
                    offsetCommitResponse = OffsetCommitResponse.ParseFrom(reader);
                }
            }
            else
            {
                var asyncSocket = this.asyncSockets[coordinator];
                offsetCommitResponse = asyncSocket.Send(offsetCommitRequest);
            }


            if (offsetCommitResponse == null)
            {
                throw new Exception("[UNHANDLED-EXCEPTION] null offsetCommitResponse");
            }

            var targetResponse = offsetCommitResponse.ResponseInfo[topic].First(r => r.PartitionId == partition);
            if (targetResponse.ErrorCode == 0)
            {
                logger.Debug(string.Format("CommitOffset: successfully commited offset to broker topic {0} partition {1} offset {2}", topic, partition, offset));

                if (setPosition)
                {
                    var target = brokerOrderedFetchRequestEntries.First(x => x.Value.Exists(e => e.Topic == topic && e.PartitionId == partition));
                    var entry = brokerOrderedFetchRequestEntries[target.Key].First(e => e.Topic == topic && e.PartitionId == partition);
                    entry.Position = offset;
                }
            }
            else
            {
                logger.Debug(string.Format("CommitOffset: offset commited failed with error {0} for topic {1} partition {2} offset {3}", targetResponse.ErrorCode, topic, partition, offset));

                if (targetResponse.ErrorCode == 12) // topic metadata to large
                {
                }
                else if (targetResponse.ErrorCode == 28) // invalid commit offset size
                {
                }
                else if (targetResponse.ErrorCode == 14) //group load in progress
                {
                }
                else if (targetResponse.ErrorCode == 15) // group coordinator not available
                {
                    // need to find the coordinator again
                }
                else if (targetResponse.ErrorCode == 16) // not coordinator for current group
                {
                }
                else if (targetResponse.ErrorCode == 25) // unknown member id
                {
                    // group balanc changed
                }

            }
        }

        public Dictionary<int, long> GetLastCommittedOffset(string topic)
        {
            var offsets = new Dictionary<int, long>();

            var groupOffsets = this.consumerGroupOffsetFunc(topic, this.config.GroupId);
            if (groupOffsets.ContainsKey(topic))
            {
                offsets = groupOffsets[topic];
            }

            return offsets;
        }

        public Dictionary<int, long> ResetCommittedOffsetToEarliest(string topic)
        {
            Dictionary<int, long> result = new Dictionary<int, long>();

            var earliestOffsets = this.dataOffsetFunc(-2);
            if (earliestOffsets.ContainsKey(topic))
            {
                // update fetch entities
                this.updateFetchEntities(earliestOffsets);

                // commit it
                this.commitOffsetAction();

                return earliestOffsets[topic];
            }

            return result;
        }

        public Dictionary<int, long> GetEarliestDataOffset(string topic)
        {
            Dictionary<int, long> result = new Dictionary<int, long>();
            var earliestOffsets = dataOffsetFunc(-2);
            if (earliestOffsets.ContainsKey(topic))
            {
                return earliestOffsets[topic];
            }

            return result;
        }

        public Dictionary<int, long> GetLatestDataOffset(string topic)
        {
            Dictionary<int, long> result = new Dictionary<int, long>();
            var latestOffsets = dataOffsetFunc(-1);
            if (latestOffsets.ContainsKey(topic))
            {
                return latestOffsets[topic];
            }

            return result;
        }

        public void SetOffsets(Dictionary<string, Dictionary<int, long>> offsets)
        {
            this.updateFetchEntities(offsets);
        }

        void initFetchAction()
        {
            foreach (var kvp in brokerOrderedFetchRequestEntries)
            {
                var bid = kvp.Key;
                var entities = kvp.Value;
                try
                {

                    if (!taskQueue.ContainsKey(kvp.Key))
                    {
                        taskQueue.Add(kvp.Key, new List<Task<object>>());
                    }

                    // clean completed tasks
                    var existingTask = taskQueue[kvp.Key].Where(t => t.IsCompleted).ToList();
                    if (!fetchResponseQueue.Any(e => e.Broker == kvp.Key))
                    {
                        foreach (var t in existingTask)
                        {
                            var result = (KafkaFetchTaskResult)t.Result;
                            taskQueue[kvp.Key].Remove(t);
                            logger.Debug("initFetchAction => task of fetch and parse response from broker " + result.BrokerId + " complete in " + TimeSpan.FromTicks(DateTime.UtcNow.Ticks - result.BeginTime.Ticks).TotalSeconds + " seconds");
                        }
                    }

                    if (taskQueue[kvp.Key].Count == 0)
                    {
                        var fo = new KafkaFetchTaskArgs();
                        fo.BrokerId = bid;
                        fo.FetchTaskId = this.GetUniqueId();
                        fo.FetchEntities = new List<KafkaFetchRequestEntry>(/*entities*/);
                        // clone the request entry so Position can be referenced without considering updates by SetOffset
                        foreach (var e in entities)
                        {
                            fo.FetchEntities.Add(new KafkaFetchRequestEntry() { BrokerId = e.BrokerId, Topic = e.Topic, PartitionId = e.PartitionId, Position = e.Position });
                        }
                        var task = Task.Factory.StartNew<object>(() =>
                        {
                            DateTime timestamp = DateTime.UtcNow;
                            fetchDataAction(fo);
                            return new KafkaFetchTaskResult() { BeginTime = timestamp, BrokerId = bid };
                        });

                        // put task to broker's task queue
                        taskQueue[kvp.Key].Add(task);
                    }
                }
                catch (Exception e)
                {
                    logger.Debug("[UNHANDLED-EXCEPTION] initFetchAction => start fetch task failed " + e.Message + " " + e.StackTrace);
                }
            }
        }

        public IEnumerable<KafkaMessage> ReadMessages()
        {

            /*
            if (!this.brokerOrderedMetadatas.Any())
            {
                // first time refresh metadata
                this.metadataAction();
            }
            */
            if (!metadataReadyEvent.WaitOne(TimeSpan.FromMinutes(5)))
            {
                this.metadataAction();
                metadataReadyEvent.Set();
            }

            // make sure there are fetch entities for fetching
            ensureFetchEntitiesAssigned();

            // make sure fetch offsets are good
            this.ensureFetchOffsets();

            // init fetch requests
            this.initFetchAction();

            DateTime startUtc = DateTime.UtcNow;

            if (this.decodeResponseUsingQueue == false)
            {
                // check whether we have response to parse for messages
                if (this.fetchResponseQueue.Count > 0)
                {
                    startUtc = DateTime.UtcNow;
                    this.fetchResponseDecodeAction();
                    logger.Debug("ReadMessages => fetchResponseDecodeAction complete in " + TimeSpan.FromTicks(DateTime.UtcNow.Ticks - startUtc.Ticks).TotalSeconds + " seconds");
                }
            }

            // return fetched messages
            List<KafkaMessage> messages = new List<KafkaMessage>();
            startUtc = DateTime.UtcNow;
            foreach (var pair in fetchedMessagesList)
            {
                var list = pair.Value;
                lock (list)
                {
                    if (list.Count > 0)
                    {
                        messages.AddRange(list);

                        // to avoid memory leak, need to delete cached messages
                        list.Clear();
                    }
                }
            }

            if (messages.Count > 0)
            {
                logger.Debug("ReadMessages => return fetched messages complete " + TimeSpan.FromTicks(DateTime.UtcNow.Ticks - startUtc.Ticks).TotalSeconds + " seconds, total count " + messages.Count);
            }

            return messages;
        }

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        private long GetUniqueId()
        {
            return Interlocked.Increment(ref this.uniqueId);
        }

        protected void Dispose(bool disposing)
        {
            if (disposing)
            {
                // commit offset before die
                try
                {
                    this.CommitOffsets();
                }
                catch (Exception e)
                {
                    logger.Debug("[UNHANDLED-EXCEPTION] Dispose : last chance to commit offset failed\r\n" + e);
                    throw e;
                }

                this.stopRunning = true;
                if (this.heartbeatingThread != null && this.heartbeatingThread.ThreadState == ThreadState.Running)
                {
                    this.heartbeatingThread.Abort();
                }

                if (this.autoCommitThread != null && this.autoCommitThread.ThreadState == ThreadState.Running)
                {
                    this.autoCommitThread.Abort();
                }

                foreach (var socket in this.sockets)
                {
                    var id = socket.Key;
                    socket.Value.Disconnect(false);
                    socket.Value.Dispose();
                }

                this.sockets = null;

                if (!this.useAsyncSocket)
                {
                    foreach (var asyncSocket in this.asyncSockets)
                    {
                        asyncSocket.Value.Dispose();
                    }

                    this.asyncSockets = null;
                }
            }
        }

        public class KafkaFetchRequestEntry
        {
            public int BrokerId { get; set; }
            public string Topic { get; set; }
            public int PartitionId { get; set; }
            public long Position { get; set; }
        }

        public class KafkaFetchTaskArgs
        {
            public long FetchTaskId;
            public int BrokerId;
            public List<KafkaFetchRequestEntry> FetchEntities;
        }
        private class KafkaFetchTaskResult
        {
            public DateTime BeginTime;
            public int BrokerId;
            public long MessageCount;
        }
        public static class KafkaCorrelationIdGenerator
        {
            private static int correlationId = 0;

            public static int GetNextId()
            {
                var newId = Interlocked.Increment(ref correlationId);
                return correlationId;
            }
        }


        public class KafkaSocketToken : IDisposable
        {
            private log4net.ILog logger = log4net.LogManager.GetLogger(typeof(KafkaSocketToken).Name);

            // This is a ref copy of the socket that owns this token
            private Socket ownersocket;

            // this stringbuilder is used to accumulate data off of the readsocket
            private StringBuilder stringbuilder;

            private Stream stream;

            // This stores the total bytes accumulated so far in the stringbuilder
            private Int32 totalbytecount;

            // We are holding an exception string in here, but not doing anything with it right now.
            public String LastError;

            private Action<byte[]> datacallback;

            private bool headerReceived;

            public DateTime BeginTime { get; set; }

            public ManualResetEvent ReceiveCompleteEvent { get; set; }
            // The read socket that creates this object sends a copy of its "parent" accept socket in as a reference
            // We also take in a max buffer size for the data to be read off of the read socket
            public KafkaSocketToken(Socket socket, Int32 bufferSize, Action<byte[]> callback = null)
            {
                headerReceived = false;
                ownersocket = socket;
                stringbuilder = new StringBuilder(bufferSize);
                datacallback = callback;
                ReminingSize = 0;
                stream = new MemoryStream();
                BeginTime = DateTime.UtcNow;
                this.ReceiveCompleteEvent = new ManualResetEvent(false);
            }

            public ApiKey ApiKey { get; set; }

            public bool HeaderReceived { get { return this.headerReceived; } }

            public int ReminingSize { get; set; }

            public int RequestId { get; set; }

            public int PacketSize { get; set; }

            // This allows us to refer to the socket that created this token's read socket
            public Socket OwnerSocket
            {
                get
                {
                    return ownersocket;
                }
            }

            static byte[] GetBytes(string str)
            {
                //byte[] bytes = new byte[str.Length * sizeof(char)];
                //System.Buffer.BlockCopy(str.ToCharArray(), 0, bytes, 0, bytes.Length);
                byte[] bytes = str.ToCharArray().Select(b => (byte)b).ToArray();
                return bytes;
            }

            static string GetString(byte[] bytes)
            {
                //char[] chars = new char[bytes.Length / sizeof(char)];
                //System.Buffer.BlockCopy(bytes, 0, chars, 0, bytes.Length);
                char[] chars = bytes.Select(c => (char)c).ToArray();
                return new string(chars);
            }

            public void ProcessData(SocketAsyncEventArgs args)
            {
                if (datacallback != null)
                {
                    logger.Debug("ProcessData => response for request " + (ApiKey)this.ApiKey + " is ready.");
                    datacallback(null);
                    this.ReceiveCompleteEvent.Set();
                }
            }

            // Do something with the received data, then reset the token for use by another connection.
            // This is called when all of the data have been received for a read socket.
            public void ProcessDataV2(SocketAsyncEventArgs args)
            {
                /*
                // Get the last message received from the client, which has been stored in the stringbuilder.
                String received = stringbuilder.ToString();

                //TODO Use message received to perform a specific operation.
                //logger.Debug("ProcessData: data received " + received + " bytes " + received.Length);

                if (datacallback != null)
                {
                    //var bytes = Encoding.ASCII.GetBytes(received);

                    var bytes = GetBytes(received);
                    logger.Debug("ProcessData => response for request " + (RequestTypes)this.RequestType + " is ready.");
                    datacallback(bytes);
                }

                //TODO: Load up a send buffer to send an ack back to the calling client
                //Byte[] sendBuffer = Encoding.ASCII.GetBytes(received);
                //args.SetBuffer(sendBuffer, 0, sendBuffer.Length);

                // Clear StringBuffer, so it can receive more data from the client.
                stringbuilder.Clear();
                stringbuilder.Length = 0;
                totalbytecount = 0;
                */

                stream.Seek(0, SeekOrigin.Begin);
                byte[] buffer = new byte[stream.Length];
                stream.Read(buffer, 0, buffer.Length);

                byte[] headerbytes = new byte[8];
                Array.Copy(buffer, 0, headerbytes, 0, 8);
                using (var reader = new KafkaBinaryReader(new MemoryStream(headerbytes)))
                {
                    PacketSize = reader.ReadInt32();
                    int correlationId = reader.ReadInt32();

                    logger.Debug("ReadSocketData => reqType " + (ApiKey)this.ApiKey + " , reqId " + this.RequestId);

                    if (correlationId != this.RequestId)
                    {
                        throw new Exception("[UNHANDLED-EXCEPTION] correlation id in response " + correlationId + " does not math id in request " + this.RequestId);
                    }
                }

                if (datacallback != null)
                {
                    logger.Debug("ProcessData => response for request " + (ApiKey)this.ApiKey + " is ready.");
                    datacallback(buffer);
                }

            }


            // This method gets the data out of the read socket and adds it to the accumulator string builder
            public bool ReadSocketData(SocketAsyncEventArgs readSocket)
            {
                Int32 bytecount = readSocket.BytesTransferred;

                //if (!headerReceived)
                //{
                //    byte[] firstPacket = new byte[bytecount];
                //    Array.Copy(readSocket.Buffer, readSocket.Offset, firstPacket, 0, bytecount);

                //    //string text = Encoding.ASCII.GetString(firstPacket);
                //    /*
                //    string text = GetString(firstPacket);

                //    stringbuilder.Append(text);
                //    */
                //    stream.Write(firstPacket, 0, bytecount);

                //    totalbytecount += bytecount;

                //    byte[] sizebytes = new byte[8];
                //    Array.Copy(firstPacket, 0, sizebytes, 0, 8);

                //    using (var reader = new KafkaBinaryReader(new MemoryStream(sizebytes)))
                //    {
                //        PacketSize = reader.ReadInt32();
                //        int correlationId = reader.ReadInt32();

                //        logger.Debug("ReadSocketData => reqType " + (RequestTypes)this.RequestType + " , reqId " + this.RequestId);

                //        if (correlationId != this.RequestId)
                //        {
                //            //throw new Exception("correlation id in response " + correlationId + " does not math id in request " + this.RequestId);
                //            return false;
                //        }

                //        ReminingSize = PacketSize - (bytecount - 4);
                //    }

                //    headerReceived = true;
                //    return true;
                //}
                //else
                {
                    try
                    {
                        //if ((totalbytecount + bytecount) > stringbuilder.Capacity)
                        //{
                        //    LastError = "Receive Buffer cannot hold the entire message for this connection.";
                        //    return false;
                        //}
                        //else
                        {
                            //string text = Encoding.ASCII.GetString(readSocket.Buffer, readSocket.Offset, bytecount);
                            byte[] data = new byte[bytecount];
                            Array.Copy(readSocket.Buffer, readSocket.Offset, data, 0, bytecount);
                            /*
                            string text = GetString(data);
                            stringbuilder.Append(text);
                            */
                            stream.Write(data, 0, bytecount);

                            totalbytecount += bytecount;
                            ReminingSize -= bytecount;
                            return true;
                        }
                    }
                    catch (Exception e)
                    {
                        logger.Debug("[UNHANDLED-EXCEPTION] ReadSocketData => failed to read data " + e.StackTrace);
                        throw;
                    }
                    return false;
                }
            }

            // This is a standard IDisposable method
            // In this case, disposing of this token closes the accept socket
            public void Dispose()
            {
                if (stream != null)
                    stream.Dispose();
            }
        }
        public class KafkaSocket : IDisposable
        {
            private log4net.ILog logger = log4net.LogManager.GetLogger(typeof(KafkaSocket).Name);
            private const int BUFFER_SIZE = KafkaConfiguration.DefaultReceiveBufferSize;
            private Socket socket;
            private IPEndPoint ipEndPoint;
            ConcurrentDictionary<int, List<byte>> receiveStreams = new ConcurrentDictionary<int, List<byte>>();


            public KafkaSocket(IPEndPoint endPoint)
            {
                this.ipEndPoint = endPoint;
                this.socket = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
                {
                    NoDelay = true,
                    ReceiveTimeout = KafkaConfiguration.DefaultReceiveTimeout,
                    SendTimeout = KafkaConfiguration.DefaultSendTimeout,
                    SendBufferSize = KafkaConfiguration.DefaultSendBufferSize,
                    ReceiveBufferSize = KafkaConfiguration.DefaultReceiveBufferSize
                };
                this.ensureConnected();
            }

            public KafkaSocket(Broker broker)
            {
                this.ipEndPoint = new IPEndPoint(IPAddress.Parse(broker.Host), broker.Port);
                this.socket = new Socket(this.ipEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                this.ensureConnected();
            }

            private void ensureConnected()
            {
                if (this.socket.Connected)
                {
                    return;
                }

                var waitEvent = new ManualResetEvent(false);
                var args = new SocketAsyncEventArgs();
                args.RemoteEndPoint = this.ipEndPoint;
                args.Completed += (sender, eventArgs) =>
                {
                    waitEvent.Set();
                };

                if (this.socket.ConnectAsync(args))
                {
                    waitEvent.Set();
                }

                waitEvent.WaitOne();
            }

            public void Dispose()
            {
                this.socket.Dispose();
            }

            public OffsetFetchResponse Send(OffsetFetchRequest request)
            {
                var id = KafkaCorrelationIdGenerator.GetNextId();
                request.CorrelationId = id;
                var bytes = request.Serialize();
                var data = this.Send(bytes, (short)request.ApiKey, id).Result;

                try
                {
                    var resp = OffsetFetchResponse.ParseFrom(new KafkaBinaryReader(new MemoryStream(data)));
                    return resp;
                }
                catch (Exception e)
                {
                    logger.Debug("[UNHANDLED-EXCEPTION] Send => failed to parse OffsetFetchResponse for reqId " + id + " from " + this.ipEndPoint + " " + e.StackTrace);
                    throw e;
                }
            }

            public SyncGroupResponse Send(SyncGroupRequest request)
            {
                var id = KafkaCorrelationIdGenerator.GetNextId();
                request.CorrelationId = id;
                var bytes = request.Serialize();
                var data = this.Send(bytes, (short)request.ApiKey, id).Result;

                try
                {
                    var resp = SyncGroupResponse.ParseFrom(new KafkaBinaryReader(new MemoryStream(data)));
                    return resp;
                }
                catch (Exception e)
                {
                    logger.Debug("[UNHANDLED-EXCEPTION] Send => failed to parse SyncGroupResponse for reqId " + id + " from " + this.ipEndPoint + " " + e.StackTrace);
                    throw e;
                }
            }

            public OffsetResponse Send(OffsetRequest request)
            {
                var id = KafkaCorrelationIdGenerator.GetNextId();
                request.CorrelationId = id;
                var bytes = request.Serialize();
                var data = this.Send(bytes, (short)request.ApiKey, id).Result;

                try
                {
                    var resp = OffsetResponse.ParseFrom(new KafkaBinaryReader(new MemoryStream(data)));
                    return resp;
                }
                catch (Exception e)
                {
                    logger.Debug("[UNHANDLED-EXCEPTION] Send => failed to parse OffsetResponse for reqId " + id + " from " + this.ipEndPoint + " " + e.StackTrace);
                    throw e;
                }
            }

            public DescribeGroupsResponse Send(DescribeGroupsRequest request)
            {
                var id = KafkaCorrelationIdGenerator.GetNextId();
                request.CorrelationId = id;
                var bytes = request.Serialize();
                var data = this.Send(bytes, (short)request.ApiKey, id).Result;

                try
                {
                    var resp = DescribeGroupsResponse.ParseFrom(new KafkaBinaryReader(new MemoryStream(data)));
                    return resp;
                }
                catch (Exception e)
                {
                    logger.Debug("[UNHANDLED-EXCEPTION] Send => failed to parse DescribeGroupsResponse for reqId " + id + " from " + this.ipEndPoint + " " + e.StackTrace);
                    throw e;
                }
            }

            public FetchResponse Send(FetchRequest request)
            {
                var id = KafkaCorrelationIdGenerator.GetNextId();
                request.CorrelationId = id;
                var bytes = request.Serialize();
                logger.Debug("Send => send Fetch reqId " + id + " to " + this.ipEndPoint);
                var data = this.Send(bytes, (short)request.ApiKey, id).Result;

                try
                {
                    var resp = FetchResponse.ParseFrom(new KafkaBinaryReader(new MemoryStream(data)));
                    logger.Debug("Send => complete Fetch reqId " + id + " to " + this.ipEndPoint);
                    return resp;
                }
                catch (Exception e)
                {
                    logger.Debug("[UNHANDLED-EXCEPTION] Send => failed to parse Fetch response for reqId " + id + " from " + this.ipEndPoint + " " + e.StackTrace);
                    throw e;
                }

            }

            public GroupCoordinatorResponse Send(GroupCoordinatorRequest request)
            {
                var id = KafkaCorrelationIdGenerator.GetNextId();
                request.CorrelationId = id;
                var bytes = request.Serialize();
                var data = this.Send(bytes, (short)request.ApiKey, id).Result;

                try
                {
                    var resp = GroupCoordinatorResponse.ParseFrom(new KafkaBinaryReader(new MemoryStream(data)));
                    return resp;
                }
                catch (Exception e)
                {
                    logger.Debug("[UNHANDLED-EXCEPTION] Send => failed to parse GroupCoordinatorResponse for reqId " + id + " from " + this.ipEndPoint + " " + e.StackTrace);
                    throw e;
                }
            }

            public HeartbeatResponse Send(HeartbeatRequest request)
            {
                var id = KafkaCorrelationIdGenerator.GetNextId();
                request.CorrelationId = id;
                var bytes = request.Serialize();
                var data = this.Send(bytes, (short)request.ApiKey, id).Result;

                try
                {
                    var resp = HeartbeatResponse.ParseFrom(new KafkaBinaryReader(new MemoryStream(data)));
                    return resp;
                }
                catch (Exception e)
                {
                    logger.Debug("[UNHANDLED-EXCEPTION] Send => failed to parse heartbeat response for reqId " + id + " from " + this.ipEndPoint + " " + e.StackTrace);
                    throw e;
                }
            }

            public JoinGroupResponse Send(JoinGroupRequest request)
            {
                var id = KafkaCorrelationIdGenerator.GetNextId();
                request.CorrelationId = id;
                var bytes = request.Serialize();
                var data = this.Send(bytes, (short)request.ApiKey, id).Result;

                try
                {
                    var resp = JoinGroupResponse.ParseFrom(new KafkaBinaryReader(new MemoryStream(data)));
                    return resp;
                }
                catch (Exception e)
                {
                    logger.Debug("[UNHANDLED-EXCEPTION] Send => failed to parse JoinGroupResponse for reqId " + id + " from " + this.ipEndPoint + " " + e.StackTrace);
                    throw e;
                }
            }

            public LeaveGroupResponse Send(LeaveGroupRequest request)
            {
                var id = KafkaCorrelationIdGenerator.GetNextId();
                request.CorrelationId = id;
                var bytes = request.Serialize();
                var data = this.Send(bytes, (short)request.ApiKey, id).Result;


                try
                {
                    var resp = LeaveGroupResponse.ParseFrom(new KafkaBinaryReader(new MemoryStream(data)));
                    return resp;
                }
                catch (Exception e)
                {
                    logger.Debug("[UNHANDLED-EXCEPTION] Send => failed to parse LeaveGroupResponse for reqId " + id + " from " + this.ipEndPoint + " " + e.StackTrace);
                    throw e;
                }
            }

            public OffsetCommitResponse Send(OffsetCommitRequest request)
            {
                var id = KafkaCorrelationIdGenerator.GetNextId();
                request.CorrelationId = id;
                var bytes = request.Serialize();
                var data = this.Send(bytes, (short)request.ApiKey, id).Result;

                try
                {
                    var resp = OffsetCommitResponse.ParseFrom(new KafkaBinaryReader(new MemoryStream(data)));
                    return resp;
                }
                catch (Exception e)
                {
                    logger.Debug("[UNHANDLED-EXCEPTION] Send => failed to parse OffsetCommitResponse for reqId " + id + " from " + this.ipEndPoint + " " + e.StackTrace);
                    throw e;
                }
            }

            public IEnumerable<TopicMetadataResponseTopicInfo> Send(TopicMetadataRequest request)
            {
                var id = KafkaCorrelationIdGenerator.GetNextId();
                request.CorrelationId = id;
                var bytes = request.Serialize();
                var data = this.Send(bytes, (short)request.ApiKey, id).Result;

                try
                {
                    var resp = TopicMetadataRequest.ParseFrom(new KafkaBinaryReader(new MemoryStream(data)));
                    return resp.TopicMetadatas;
                }
                catch (Exception e)
                {
                    logger.Debug("[UNHANDLED-EXCEPTION] Send => failed to parse TopicMetadataRequest for reqId " + id + " from " + this.ipEndPoint + " " + e.StackTrace);
                    throw e;
                }
            }

            private Task<byte[]> Send(byte[] bytes, short reqType, int reqId)
            {
                return Task.Factory.StartNew<byte[]>(() =>
                {
                    ensureConnected();

                    if (!receiveStreams.ContainsKey(reqId))
                    {
                        receiveStreams.TryAdd(reqId, new List<byte>());
                    }

                    byte[] received = null;

                    SocketAsyncEventArgs sendArgs = null;
                    KafkaSocketToken token = null;

                    token = new KafkaSocketToken(this.socket, BUFFER_SIZE, data =>
                    {
                        if (data == null)
                        {
                            List<byte> stream;
                            if (receiveStreams.TryGetValue(reqId, out stream))
                            {
                                received = stream.ToArray();
                            }

                            if (receiveStreams.TryRemove(reqId, out stream))
                            {
                                stream.Clear();
                            }
                        }
                        else
                        {
                            received = data;
                        }

                    });

                    lock (this)
                    {

                        token.ApiKey = (ApiKey)reqType;
                        token.RequestId = reqId;
                        sendArgs = new SocketAsyncEventArgs();
                        sendArgs.SetBuffer(bytes, 0, bytes.Length);
                        sendArgs.Completed += new EventHandler<SocketAsyncEventArgs>(OnIOCompleted);
                        sendArgs.UserToken = token;
                        bool willRaiseEvent = token.OwnerSocket.SendAsync(sendArgs);
                        if (!willRaiseEvent)
                        {
                            ProcessSend(sendArgs);
                        }

                    }

                    logger.Debug("Send => request " + (ApiKey)reqType + " is sent to " + ipEndPoint + ", now wait for response");
                    token.ReceiveCompleteEvent.WaitOne();
                    logger.Debug("Send => request " + (ApiKey)reqType + " response received from " + ipEndPoint);


                    if (token != null)
                        token.Dispose();
                    if (sendArgs != null)
                        sendArgs.Dispose();

                    return received;
                });
            }

            private void OnIOCompleted(object sender, SocketAsyncEventArgs e)
            {
                switch (e.LastOperation)
                {
                    case SocketAsyncOperation.Receive:
                        this.ProcessReceive(e);
                        break;
                    case SocketAsyncOperation.Send:
                        ProcessSend(e);
                        break;
                    default:
                        throw new ArgumentException("[UNHANDLED-EXCEPTION] The last operation completed on the socket was not a receive");
                }
            }


            private void ProcessSend(SocketAsyncEventArgs e)
            {
                if (e.SocketError == SocketError.Success)
                {
                    lock (this)
                    {
                        KafkaSocketToken token = (KafkaSocketToken)(e.UserToken);
                        SocketAsyncEventArgs receiveArgs = new SocketAsyncEventArgs();
                        receiveArgs.SetBuffer(new byte[BUFFER_SIZE], 0, BUFFER_SIZE);
                        receiveArgs.UserToken = token;
                        receiveArgs.Completed += OnIOCompleted;

                        bool willRaiseEvent = token.OwnerSocket.ReceiveAsync(receiveArgs);
                        if (!willRaiseEvent)
                        {
                            ProcessReceive(receiveArgs);
                        }
                    }
                }
                else
                {
                    logger.Debug("ProcessSend => failed with error " + e.SocketError);
                    throw new Exception("[UNHANDLED-EXCEPTION] ProcessSend => failed with error " + e.SocketError);
                }
            }


            private void ProcessReceive(SocketAsyncEventArgs socketArgs)
            {

                // if BytesTransferred is 0, then the remote end closed the connection
                if (socketArgs.BytesTransferred > 0)
                {
                    //SocketError.Success indicates that the last operation on the underlying socket succeeded
                    if (socketArgs.SocketError == SocketError.Success)
                    {
                        KafkaSocketToken token = socketArgs.UserToken as KafkaSocketToken;

                        var id = token.RequestId;
                        Socket ownedSocket = token.OwnerSocket;

                        logger.Debug("ProcessReceive => receive data from " + this.ipEndPoint + " for reqId " + id + " reqType " +
                                     (ApiKey)token.ApiKey + " current bytes " + socketArgs.BytesTransferred);

                        lock (this)
                        {
                            var stream = receiveStreams[id];
                            var buffer = new byte[socketArgs.BytesTransferred];
                            Array.Copy(socketArgs.Buffer, socketArgs.Offset, buffer, 0, socketArgs.BytesTransferred);
                            stream.AddRange(buffer);
                        }

                        if (ownedSocket.Available == 0)
                        {

                            int expectedSize = 0;
                            int expectedId = 0;
                            lock (this)
                            {
                                byte[] header = new byte[8];
                                Array.Copy(receiveStreams[id].ToArray(), 0, header, 0, 8);
                                using (var reader = new KafkaBinaryReader(new MemoryStream(header)))
                                {
                                    expectedSize = reader.ReadInt32();
                                    expectedId = reader.ReadInt32();
                                }
                                logger.Debug("ProcessReceive => receive data complete from " + this.ipEndPoint + " for reqId " +
                                         id + " reqType " +
                                         (ApiKey)token.ApiKey + " with expected size  " + expectedSize + " expected id " + expectedId + " actual size " + (receiveStreams[id].Count - 4) + " in " +
                                         TimeSpan.FromTicks(DateTime.UtcNow.Ticks - token.BeginTime.Ticks)
                                             .TotalSeconds + " seconds ");

                                if (receiveStreams[id].Count - 4 == expectedSize)
                                {
                                    token.ProcessData(socketArgs);
                                    return;
                                }
                            }

                        }

                        lock (this)
                        {
                            bool IOPending = ownedSocket.ReceiveAsync(socketArgs);
                            if (!IOPending)
                            {
                                ProcessReceive(socketArgs);
                            }
                        }
                    }
                    else
                    {
                        logger.Debug("ProcessReceive => failed with error " + socketArgs.SocketError);
                        throw new Exception("[UNHANDLED-EXCEPTION] ProcessReceive => unhandled exception : socket error " + socketArgs.SocketError);
                    }
                }
                else
                {
                    logger.Debug("ProcessReceive => no data available");
                }
            }

            private void ProcessReceiveV2(SocketAsyncEventArgs readSocket)
            {
                KafkaSocketToken token = readSocket.UserToken as KafkaSocketToken;

                // if BytesTransferred is 0, then the remote end closed the connection
                if (readSocket.BytesTransferred > 0)
                {
                    //SocketError.Success indicates that the last operation on the underlying socket succeeded
                    if (readSocket.SocketError == SocketError.Success)
                    {
                        logger.Debug("ProcessReceive => receive data from " + this.ipEndPoint + " for request " +
                                     (ApiKey)token.ApiKey + " current bytes " + readSocket.BytesTransferred);

                        if (token.ReadSocketData(readSocket))
                        {
                            logger.Debug("ProcessReceive => receive data from " + this.ipEndPoint + " for request " +
                                         (ApiKey)token.ApiKey + " remining bytes " + token.ReminingSize);

                            Socket readsocket = token.OwnerSocket;
                            var available = readsocket.Available;
                            // If the read socket is empty, we can do something with the data that we accumulated
                            // from all of the previous read requests on this socket
                            if (readsocket.Available == 0)
                            //if (token.ReminingSize == 0)
                            {
                                logger.Debug("ProcessReceive => receive data from " + this.ipEndPoint + " for request " +
                                             (ApiKey)token.ApiKey + " is completed.");
                                token.ProcessData(readSocket);
                                return;
                            }

                            // Start another receive request and immediately check to see if the receive is already complete
                            // Otherwise OnIOCompleted will get called when the receive is complete
                            // We are basically calling this same method recursively until there is no more data
                            // on the read socket
                            bool IOPending = readsocket.ReceiveAsync(readSocket);
                            if (!IOPending)
                            {
                                ProcessReceive(readSocket);
                            }
                        }
                        else
                        {
                            logger.Debug("ProcessReceive => failed to read data");
                            throw new Exception("[UNHANDLED-EXCEPTION] ProcessReceive => unhandled exception : failed to read data");
                        }
                    }
                    else
                    {
                        logger.Debug("ProcessReceive => failed with error " + readSocket.SocketError);
                        throw new Exception("ProcessReceive => unhandled exception : socket error " + readSocket.SocketError);
                    }
                }
                else
                {
                    logger.Debug("ProcessReceive => no data available");
                }
            }

        }

    }

}