using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DotNetKafkaClient
{
    public class FetchResponse : KafkaResponse
    {
        public static log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(FetchResponse).Name);

        public FetchResponse(int correlationId, IEnumerable<FetchResponseTopicInfo> data)
        {
            //Guard.NotNull(data, "data");
            this.CorrelationId = correlationId;
            this.TopicDataDict = data.GroupBy(x => x.Topic, x => x)
                .ToDictionary(x => x.Key, x => x.ToList().FirstOrDefault());
        }
        public FetchResponse(int correlationId, IEnumerable<FetchResponseTopicInfo> data, int size)
        {
            //Guard.NotNull(data, "data");
            this.CorrelationId = correlationId;
            this.TopicDataDict = data.GroupBy(x => x.Topic, x => x)
                .ToDictionary(x => x.Key, x => x.ToList().FirstOrDefault());
            this.Size = size;
        }

        public override int Size { get; set; }
        public override int CorrelationId { get; set; }
        public Dictionary<string, FetchResponseTopicInfo> TopicDataDict { get; private set; }

        public static FetchResponse ParseFrom(KafkaBinaryReader reader)
        {
            FetchResponse result = null;

            DateTime startUtc = DateTime.UtcNow;

            int size = 0, correlationId = 0, dataCount = 0;
            try
            {
                size = reader.ReadInt32();
                Logger.Debug("FetchResponse.ParseFrom: read size byte after " + TimeSpan.FromTicks(DateTime.UtcNow.Ticks - startUtc.Ticks).TotalSeconds + " seconds, packet size " + size);

                startUtc = DateTime.UtcNow;
                byte[] remainingBytes = reader.ReadBytes(size);
                Logger.Debug("FetchResponse.ParseFrom: read remaining bytes after " + TimeSpan.FromTicks(DateTime.UtcNow.Ticks - startUtc.Ticks).TotalSeconds + " seconds");

                startUtc = DateTime.UtcNow;
                KafkaBinaryReader dataReader = new KafkaBinaryReader(new MemoryStream(remainingBytes));

                correlationId = dataReader.ReadInt32();
                dataCount = dataReader.ReadInt32();
                var data = new FetchResponseTopicInfo[dataCount];

                // !!! improvement !!!
                // just receive the bytes, and try to parse them later
                // directly parse the record here, or just keep the bytes to speed up the fetch response
                for (int i = 0; i < dataCount; i++)
                {
                    var topic = dataReader.ReadShortString();
                    var partitionCount = dataReader.ReadInt32();

                    startUtc = DateTime.UtcNow;
                    var partitions = new FetchResponsePartitionInfo[partitionCount];
                    for (int j = 0; j < partitionCount; j++)
                    {
                        var partition = dataReader.ReadInt32();
                        var error = dataReader.ReadInt16();
                        var highWatermark = dataReader.ReadInt64();
                        var messageSetSize = dataReader.ReadInt32();
                        var messageSetBytes = dataReader.ReadBytes(messageSetSize);

                        Logger.Debug("FetchResponse.ParseFrom: topic " + topic + " partition " + partition + " should get records in " + messageSetSize + " bytes, error " + error + " watermark " + highWatermark);
                        partitions[j] = new FetchResponsePartitionInfo(partition, error, highWatermark,
                            messageSetBytes);
                    }
                    Logger.Debug("FetchResponse.ParseFrom: read " + partitionCount + " partitions for segment " + (i + 1) + " use " + TimeSpan.FromTicks(DateTime.UtcNow.Ticks - startUtc.Ticks).TotalSeconds + " seconds");

                    data[i] = new FetchResponseTopicInfo(topic, partitions);
                }

                result = new FetchResponse(correlationId, data, size);

                Logger.Debug("FetchResponse.ParseFrom: read bytes into structure complete after " + TimeSpan.FromTicks(DateTime.UtcNow.Ticks - startUtc.Ticks).TotalSeconds + " seconds");
            }
            catch (OutOfMemoryException mex)
            {
                Logger.Error(
                    string.Format(
                        "OOM Error. Data values were: size: {0}, correlationId: {1}, dataCound: {2}.\r\nFull Stack of exception: {3}",
                        size, correlationId, dataCount, mex.StackTrace));
                throw;
            }
            catch (Exception e)
            {
                Logger.Debug("FetchResponse.ParseFrom: parse response failed\r\n" + e);
                throw;
            }

            return result;
        }
        
    }
}