using System;
using System.Globalization;

namespace DotNetKafkaClient
{
    public static class CompressionCodecsHelper
    {
        public static CompressionCodecs GetCompressionCodec(int codec)
        {
            switch (codec)
            {
                case 0:
                    return CompressionCodecs.NoCompressionCodec;
                case 1:
                    return CompressionCodecs.GZIPCompressionCodec;
                case 2:
                    return CompressionCodecs.SnappyCompressionCodec;
                default:
                    throw new UnknownCodecException(String.Format(
                        CultureInfo.CurrentCulture,
                        "{0} is an unknown compression codec",
                        codec));
            }
        }

        public static byte GetCompressionCodecValue(CompressionCodecs compressionCodec)
        {
            switch (compressionCodec)
            {
                case CompressionCodecs.SnappyCompressionCodec:
                    return (byte)2;
                case CompressionCodecs.DefaultCompressionCodec:
                case CompressionCodecs.GZIPCompressionCodec:
                    return (byte)1;
                case CompressionCodecs.NoCompressionCodec:
                    return (byte)0;
                default:
                    throw new UnknownCodecException(String.Format(
                        CultureInfo.CurrentCulture,
                        "{0} is an unknown compression codec",
                        compressionCodec));
            }
        }
    }
}