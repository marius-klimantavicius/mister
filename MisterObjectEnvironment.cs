using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using FASTER.core;

namespace Marius.Mister
{
    public unsafe class MisterObjectEnvironment<TValue, TValueObjectSource> : IFunctions<MisterObject, MisterObject, byte[], TValue, object>
        where TValueObjectSource : struct, IMisterObjectSource
    {
        private readonly IMisterSerializer<TValue, TValueObjectSource> _serializer;

        public MisterObjectEnvironment(IMisterSerializer<TValue, TValueObjectSource> serializer)
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

        public void DeleteCompletionCallback(ref MisterObject key, object ctx)
        {
            if (ctx == null)
                return;

            var tcs = Unsafe.As<TaskCompletionSource<MisterVoid>>(ctx);
            tcs.TrySetResult(MisterVoid.Value);
        }

        public void ReadCompletionCallback(ref MisterObject key, ref byte[] input, ref TValue output, object ctx, Status status)
        {
            if (ctx == null)
                return;

            var tcs = Unsafe.As<TaskCompletionSource<TValue>>(ctx);
            if (status == Status.ERROR)
                tcs.TrySetException(new Exception());
            else
                tcs.TrySetResult(output);
        }

        public void RMWCompletionCallback(ref MisterObject key, ref byte[] input, object ctx, Status status)
        {
        }

        public void UpsertCompletionCallback(ref MisterObject key, ref MisterObject value, object ctx)
        {
            if (ctx == null)
                return;

            var tcs = Unsafe.As<TaskCompletionSource<MisterVoid>>(ctx);
            tcs.TrySetResult(MisterVoid.Value);
        }
    }
}
