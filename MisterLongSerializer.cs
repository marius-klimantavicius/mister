using System;
using System.Buffers;

namespace Marius.Mister
{
    public unsafe class MisterLongSerializer : IMisterSerializer<long, MisterPoolBufferObjectSource>
    {
        public MisterPoolBufferObjectSource Serialize(long value)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(12);
            fixed (byte* ptr = &buffer[4])
                *(long*)ptr = value;

            return new MisterPoolBufferObjectSource(buffer, 8);
        }

        public long Deserialize(ref byte value, int length)
        {
            if (length == 0)
                return 0L;

            if (length != 8)
                throw new ArgumentOutOfRangeException(nameof(length));

            fixed (byte* ptr = &value)
                return *(long*)ptr;
        }
    }
}
