using System;
using System.IO;
using FASTER.core;

namespace Marius.Mister
{
    public unsafe class MisterObjectEnvironment<TValue, TValueSerializerScope> : IFunctions<MisterObject, MisterObject, byte[], TValue, Empty>
        where TValueSerializerScope: struct, IMisterObjectSource
    {
        private readonly IMisterSerializer<TValue, TValueSerializerScope> _serializer;

        public MisterObjectEnvironment(IMisterSerializer<TValue, TValueSerializerScope> serializer)
        {
            _serializer = serializer;
        }

        public void SingleReader(ref MisterObject key, ref byte[] input, ref MisterObject value, ref TValue dst)
        {
            dst = _serializer.Deserialize(ref value.Data, value.Length);
        }

        public void ConcurrentReader(ref MisterObject key, ref byte[] input, ref MisterObject value, ref TValue dst)
        {
            dst = _serializer.Deserialize(ref value.Data, value.Length);
        }

        public void SingleWriter(ref MisterObject key, ref MisterObject src, ref MisterObject dst)
        {
            var length = src.Length;
            dst.Length = length;
            fixed (byte* source = &src.Data, destination = &dst.Data)
            {
                Buffer.MemoryCopy(source, destination, length, length);
            }
        }

        public void ConcurrentWriter(ref MisterObject key, ref MisterObject src, ref MisterObject dst)
        {
            var length = src.Length;
            dst.Length = length;
            fixed (byte* source = &src.Data, destination = &dst.Data)
            {
                Buffer.MemoryCopy(source, destination, length, length);
            }
        }

        public void CopyUpdater(ref MisterObject key, ref byte[] input, ref MisterObject oldValue, ref MisterObject newValue)
        {
            throw new NotImplementedException();
        }

        public void InitialUpdater(ref MisterObject key, ref byte[] input, ref MisterObject value)
        {
            throw new NotImplementedException();
        }

        public void InPlaceUpdater(ref MisterObject key, ref byte[] input, ref MisterObject value)
        {
            throw new NotImplementedException();
        }

        public void CheckpointCompletionCallback(Guid sessionId, long serialNum)
        {
        }

        public void DeleteCompletionCallback(ref MisterObject key, Empty ctx)
        {
        }

        public void ReadCompletionCallback(ref MisterObject key, ref byte[] input, ref TValue output, Empty ctx, Status status)
        {
        }

        public void RMWCompletionCallback(ref MisterObject key, ref byte[] input, Empty ctx, Status status)
        {
        }

        public void UpsertCompletionCallback(ref MisterObject key, ref MisterObject value, Empty ctx)
        {
        }
    }

    public unsafe class MisterObjectEnvironment<TValue> : IFunctions<MisterObject, MisterObject, byte[], TValue, Empty>
    {
        private readonly IMisterStreamSerializer<TValue> _serializer;

        public MisterObjectEnvironment(IMisterStreamSerializer<TValue> serializer)
        {
            _serializer = serializer;
        }

        public void SingleReader(ref MisterObject key, ref byte[] input, ref MisterObject value, ref TValue dst)
        {
            var length = value.Length;
            fixed (byte* valuePointer = &value.Data)
            {
                var stream = new UnmanagedMemoryStream(valuePointer, length);
                dst = _serializer.Deserialize(stream);
            }
        }

        public void ConcurrentReader(ref MisterObject key, ref byte[] input, ref MisterObject value, ref TValue dst)
        {
            var length = value.Length;
            fixed (byte* valuePointer = &value.Data)
            {
                var stream = new UnmanagedMemoryStream(valuePointer, length);
                dst = _serializer.Deserialize(stream);
            }
        }

        public void SingleWriter(ref MisterObject key, ref MisterObject src, ref MisterObject dst)
        {
            var length = src.Length;
            dst.Length = length;
            fixed (byte* source = &src.Data, destination = &dst.Data)
            {
                Buffer.MemoryCopy(source, destination, length, length);
            }
        }

        public void ConcurrentWriter(ref MisterObject key, ref MisterObject src, ref MisterObject dst)
        {
            var length = src.Length;
            dst.Length = length;
            fixed (byte* source = &src.Data, destination = &dst.Data)
            {
                Buffer.MemoryCopy(source, destination, length, length);
            }
        }

        public void CopyUpdater(ref MisterObject key, ref byte[] input, ref MisterObject oldValue, ref MisterObject newValue)
        {
            throw new NotImplementedException();
        }

        public void InitialUpdater(ref MisterObject key, ref byte[] input, ref MisterObject value)
        {
            throw new NotImplementedException();
        }

        public void InPlaceUpdater(ref MisterObject key, ref byte[] input, ref MisterObject value)
        {
            throw new NotImplementedException();
        }

        public void CheckpointCompletionCallback(Guid sessionId, long serialNum)
        {
        }

        public void DeleteCompletionCallback(ref MisterObject key, Empty ctx)
        {
        }

        public void ReadCompletionCallback(ref MisterObject key, ref byte[] input, ref TValue output, Empty ctx, Status status)
        {
        }

        public void RMWCompletionCallback(ref MisterObject key, ref byte[] input, Empty ctx, Status status)
        {
        }

        public void UpsertCompletionCallback(ref MisterObject key, ref MisterObject value, Empty ctx)
        {
        }
    }
}
