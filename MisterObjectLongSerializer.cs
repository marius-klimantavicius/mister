using System;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace Marius.Mister
{
    public unsafe class MisterObjectLongSerializer : IMisterObjectSerializer<long, MisterArrayPoolObjectSource>
    {
        public MisterArrayPoolObjectSource Serialize(long value)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(12);
            fixed (byte* ptr = &buffer[4])
                *(long*)ptr = value;

            return new MisterArrayPoolObjectSource(buffer, 8);
        }

        public long Deserialize(ref MisterObject misterObject)
        {
            var length = misterObject.Length;

            if (length == 0)
                return 0L;

            if (length != 8)
                throw new ArgumentOutOfRangeException(nameof(length));

            ref var value = ref Unsafe.As<byte, long>(ref misterObject.Data);
            return value;
        }
    }
}
