﻿using System;
using System.Buffers;

namespace Marius.Mister
{
    public unsafe class MisterObjectStringSerializer : IMisterObjectSerializer<string, MisterArrayPoolObjectSource>
    {
        public MisterArrayPoolObjectSource Serialize(string value)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(value.Length * sizeof(char) + 4);
            fixed (byte* ptr = &buffer[4])
            fixed (char* data = value)
                Buffer.MemoryCopy(data, ptr, buffer.Length - 4, value.Length * sizeof(char));

            return new MisterArrayPoolObjectSource(buffer, value.Length * sizeof(char));
        }

        public string Deserialize(ref MisterObject misterObject)
        {
            ref var value = ref misterObject.Data;
            var length = misterObject.Length;

            fixed (byte* ptr = &value)
                return new string((char*)ptr, 0, length / sizeof(char));
        }
    }
}
