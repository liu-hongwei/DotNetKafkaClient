using System;
using System.IO;

namespace DotNetKafkaClient
{
    public struct SnappyByteBuffer
    {
        byte[] buffer;
        uint offset;
        readonly int length;

        public static SnappyByteBuffer NewAsync(byte[] buffer)
        {
            return new SnappyByteBuffer(buffer, 0, buffer.Length);
        }

        public static SnappyByteBuffer NewAsync(byte[] buffer, int offset, int length)
        {
            return new SnappyByteBuffer(buffer, (uint)offset, length);
        }

        public static SnappyByteBuffer NewSync(byte[] buffer)
        {
            return new SnappyByteBuffer(buffer, 0x80000000u, buffer.Length);
        }

        public static SnappyByteBuffer NewSync(byte[] buffer, int offset, int length)
        {
            return new SnappyByteBuffer(buffer, (((uint)offset) | 0x80000000u), length);
        }

        public static SnappyByteBuffer NewEmpty()
        {
            return new SnappyByteBuffer(SnappyBitArrayHelper.EmptyByteArray, 0, 0);
        }

        private SnappyByteBuffer(byte[] buffer, uint offset, int length)
        {
            this.buffer = buffer;
            this.offset = offset;
            this.length = length;
        }

        public byte[] Buffer { get { return this.buffer; } }
        public int Offset { get { return (int)(this.offset & 0x7fffffffu); } }
        public int Length { get { return this.length; } }
        public bool AsyncSafe { get { return (this.offset & 0x80000000u) == 0u; } }

        public SnappyByteBuffer ToAsyncSafe()
        {
            if (AsyncSafe) return this;
            var copy = new byte[this.length];
            Array.Copy(this.buffer, Offset, copy, 0, this.length);
            return NewAsync(copy);
        }

        public void MakeAsyncSafe()
        {
            if (AsyncSafe) return;
            var copy = new byte[this.length];
            Array.Copy(this.buffer, Offset, copy, 0, this.length);
            this.buffer = copy;
            this.offset = 0;
        }

        public SnappyByteBuffer ResizingAppend(SnappyByteBuffer append)
        {
            if (AsyncSafe)
            {
                if (Offset + Length + append.Length <= Buffer.Length)
                {
                    Array.Copy(append.Buffer, append.Offset, Buffer, Offset + Length, append.Length);
                    return NewAsync(Buffer, Offset, Length + append.Length);
                }
            }
            var newCapacity = Math.Max(Length + append.Length, Length * 2);
            var newBuffer = new byte[newCapacity];
            Array.Copy(Buffer, Offset, newBuffer, 0, Length);
            Array.Copy(append.Buffer, append.Offset, newBuffer, Length, append.Length);
            return NewAsync(newBuffer, 0, Length + append.Length);
        }

        internal ArraySegment<byte> ToArraySegment()
        {
            return new ArraySegment<byte>(Buffer, Offset, Length);
        }

        internal byte[] ToByteArray()
        {
            var safeSelf = ToAsyncSafe();
            var buf = safeSelf.Buffer ?? SnappyBitArrayHelper.EmptyByteArray;
            if (safeSelf.Offset == 0 && safeSelf.Length == buf.Length)
            {
                return buf;
            }
            var copy = new byte[safeSelf.Length];
            Array.Copy(safeSelf.Buffer, safeSelf.Offset, copy, 0, safeSelf.Length);
            return copy;
        }

        internal MemoryStream ToStream()
        {
            return new MemoryStream(this.ToByteArray());
        }
    }
}