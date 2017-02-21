namespace DotNetKafkaClient
{
    public class KafkaMessage
    {
        private static log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(KafkaMessage).Name);

        public const int DefaultHeaderSize =
            DefaultMagicLength + DefaultCrcLength + DefaultAttributesLength + DefaultKeySizeLength +
            DefaultValueSizeLength;

        private const byte DefaultMagicValue = 0;


        /// <summary>
        /// Need set magic to 1 while compress,
        /// See https://cwiki.apache.org/confluence/display/KAFKA/Wire+Format for detail
        /// </summary>
        private const byte MagicValueWhenCompress = 1;

        private const byte DefaultMagicLength = 1;
        private const byte DefaultCrcLength = 4;
        private const byte MagicOffset = DefaultCrcLength;
        private const byte DefaultAttributesLength = 1;
        private const byte DefaultKeySizeLength = 4;
        private const byte DefaultValueSizeLength = 4;
        private const byte CompressionCodeMask = 3;

        private long _offset = -1;


        public int Crc { get; set; }

        /// <summary>
        /// Gets the magic bytes.
        /// </summary>
        public byte Magic { get; private set; }

        /// <summary>
        /// Gets the Attributes for the message.
        /// </summary>
        public byte Attributes { get; private set; }

        public byte[] Key { get; private set; }
        /// <summary>
        /// Gets the payload.
        /// </summary>
        public byte[] Content { get; private set; }

        /// <summary>
        /// Gets the total size of message.
        /// </summary>
        public int Size { get; private set; }


        public long Offset
        {
            get { return _offset; }
            set { _offset = value; }
        }

        /// <summary>
        /// When produce data, do not need set this field.
        /// When consume data, need set this field.
        /// </summary>
        public int? PartitionId { get; set; }
        public KafkaMessage(byte[] payload)
            : this(payload, null, CompressionCodecs.NoCompressionCodec)
        {
            //Guard.NotNull(payload, "payload");
        }

        public KafkaMessage(byte[] payload, CompressionCodecs compressionCodec)
            : this(payload, null, compressionCodec)
        {
            //Guard.NotNull(payload, "payload");
        }
        public KafkaMessage(byte[] payload, byte[] key, CompressionCodecs compressionCodec)
        {
            //Guard.NotNull(payload, "payload");

            int length = DefaultHeaderSize + payload.Length;
            Key = key;
            if (key != null)
            {
                length += key.Length;
            }

            this.Content = payload;
            this.Magic = DefaultMagicValue;
            if (compressionCodec != CompressionCodecs.NoCompressionCodec)
            {
                this.Attributes |=
                    (byte)(CompressionCodeMask & CompressionCodecsHelper.GetCompressionCodecValue(compressionCodec));

                // It seems that the java producer uses magic 0 for compressed messages, so we are sticking with 0 for now
                // this.Magic = MagicValueWhenCompress;
            }

            this.Size = length;
        }
    }
}