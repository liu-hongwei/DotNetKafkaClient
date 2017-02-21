using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace DotNetKafkaClient
{
    public class KafkaBinaryReader : BinaryReader
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="KafkaBinaryReader"/> class
        /// using big endian bytes order for primive types and UTF-8 encoding for strings.
        /// </summary>
        /// <param name="input">
        /// The input stream.
        /// </param>
        public KafkaBinaryReader(Stream input)
            : base(input)
        {
        }

        /// <summary>
        /// Resets position pointer.
        /// </summary>
        /// <param name="disposing">
        /// Not used
        /// </param>
        protected override void Dispose(bool disposing)
        {
            if (this.BaseStream.CanSeek)
            {
                this.BaseStream.Position = 0;
            }
        }

        /// <summary>
        /// Reads two-bytes signed integer from the current stream using big endian bytes order 
        /// and advances the stream position by two bytes
        /// </summary>
        /// <returns>
        /// The two-byte signed integer read from the current stream.
        /// </returns>
        public override short ReadInt16()
        {
            short value = base.ReadInt16();
            short currentOrdered = IPAddress.NetworkToHostOrder(value);
            return currentOrdered;
        }

        /// <summary>
        /// Reads four-bytes signed integer from the current stream using big endian bytes order 
        /// and advances the stream position by four bytes
        /// </summary>
        /// <returns>
        /// The four-byte signed integer read from the current stream.
        /// </returns>
        public override int ReadInt32()
        {
            int value = base.ReadInt32();
            int currentOrdered = IPAddress.NetworkToHostOrder(value);
            return currentOrdered;
        }
        [CLSCompliant(false)]
        public override uint ReadUInt32()
        {
            byte[] value = base.ReadBytes(4);
            return BitConverter.ToUInt32(value.Reverse().ToArray(), 0);
        }

        /// <summary>
        /// Reads eight-bytes signed integer from the current stream using big endian bytes order 
        /// and advances the stream position by eight bytes
        /// </summary>
        /// <returns>
        /// The eight-byte signed integer read from the current stream.
        /// </returns>
        public override long ReadInt64()
        {
            long value = base.ReadInt64();
            long currentOrdered = IPAddress.NetworkToHostOrder(value);
            return currentOrdered;
        }

        /// <summary>
        /// Reads four-bytes signed integer from the current stream using big endian bytes order 
        /// and advances the stream position by four bytes
        /// </summary>
        /// <returns>
        /// The four-byte signed integer read from the current stream.
        /// </returns>
        public override int Read()
        {
            int value = base.Read();
            int currentOrdered = IPAddress.NetworkToHostOrder(value);
            return currentOrdered;
        }

        /// <summary>
        /// Reads fixed-length short string from underlying stream using given encoding.
        /// </summary>
        /// <param name="encoding">
        /// The encoding to use.
        /// </param>
        /// <returns>
        /// The read string.
        /// </returns>
        public string ReadShortString(string encoding = KafkaRequest.DefaultEncoding)
        {
            short length = this.ReadInt16();
            if (length == -1)
            {
                return null;
            }

            var bytes = this.ReadBytes(length);
            Encoding encoder = Encoding.GetEncoding(encoding);
            return encoder.GetString(bytes);
        }

        public bool DataAvailable
        {
            get
            {
                if (this.BaseStream is NetworkStream)
                {
                    return ((NetworkStream)this.BaseStream).DataAvailable;
                }

                return this.BaseStream.Length != this.BaseStream.Position;
            }
        }
    }
}