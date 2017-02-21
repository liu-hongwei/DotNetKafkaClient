using System;

namespace DotNetKafkaClient
{
    public class FetchResponsePartitionInfo
    {
        private log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(FetchResponsePartitionInfo).Name);

        public const byte DefaultPartitionIdSize = 4;
        public const byte DefaultMessagesSizeSize = 4;

        public int Partition { get; private set; }
        public long HighWaterMark { get; private set; }
        public KafkaErrorCodes Error { get; private set; }

        public byte[] RecordSet { get; set; }

        public FetchResponsePartitionInfo(int partition, short error, long highWaterMark, byte[] recordSet)
        {
            this.Partition = partition;

            this.Error = (KafkaErrorCodes)(error);
            this.HighWaterMark = highWaterMark;

            if (recordSet == null || recordSet.Length <= 0)
            {
                Logger.Debug("PartitionData => partition " + partition + " get null or empty recrod set");
            }
            else
            {
                this.RecordSet = new byte[recordSet.Length];
                Buffer.BlockCopy(recordSet, 0, this.RecordSet, 0, recordSet.Length);
            }
        }

        //public int SizeInBytes
        //{
        //    get { return DefaultPartitionIdSize + DefaultMessagesSizeSize + this.MessageSet.SetSize; }
        //}

    }
}