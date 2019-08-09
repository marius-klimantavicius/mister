using System;
using System.Buffers;

namespace Marius.Mister
{
    public unsafe class MisterStringSerializer : IMisterSerializer<string, MisterPoolBufferObjectSource>
    {
        public MisterPoolBufferObjectSource Serialize(string value)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(value.Length * sizeof(char) + 4);
            fixed (byte* ptr = &buffer[4])
            fixed (char* data = value)
                Buffer.MemoryCopy(data, ptr, buffer.Length - 4, value.Length * sizeof(char));

            return new MisterPoolBufferObjectSource(buffer, value.Length * sizeof(char));
        }

        public string Deserialize(ref byte value, int length)
        {
            fixed (byte* ptr = &value)
                return new string((char*)ptr, 0, length / sizeof(char));
        }
    }
}
