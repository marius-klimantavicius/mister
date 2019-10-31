using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using FASTER.core;

namespace Marius.Mister
{
    public unsafe class MisterObjectEnvironment<TValue, TValueAtomSource> : IFunctions<MisterObject, MisterObject, byte[], TValue, object>
        where TValueAtomSource : struct, IMisterAtomSource<MisterObject>
    {
        private readonly IMisterSerializer<TValue, MisterObject, TValueAtomSource> _serializer;

        public MisterObjectEnvironment(IMisterSerializer<TValue, MisterObject, TValueAtomSource> serializer)
        {
            _serializer = serializer;
        }

        public void SingleReader(ref MisterObject key, ref byte[] input, ref MisterObject value, ref TValue dst)
        {
            dst = _serializer.Deserialize(ref value);
        }

        public void ConcurrentReader(ref MisterObject key, ref byte[] input, ref MisterObject value, ref TValue dst)
        {
            dst = _serializer.Deserialize(ref value);
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

        public bool ConcurrentWriter(ref MisterObject key, ref MisterObject src, ref MisterObject dst)
        {
            return false;
        }

        public void CopyUpdater(ref MisterObject key, ref byte[] input, ref MisterObject oldValue, ref MisterObject newValue)
        {
            throw new NotImplementedException();
        }

        public void InitialUpdater(ref MisterObject key, ref byte[] input, ref MisterObject value)
        {
            throw new NotImplementedException();
        }

        public bool InPlaceUpdater(ref MisterObject key, ref byte[] input, ref MisterObject value)
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

            var tcs = Unsafe.As<IMisterNotifyCompletion>(ctx);
            tcs.SetResult();
        }

        public void ReadCompletionCallback(ref MisterObject key, ref byte[] input, ref TValue output, object ctx, Status status)
        {
            if (ctx == null)
                return;

            var tcs = Unsafe.As<IMisterNotifyCompletion<TValue>>(ctx);
            if (status == Status.ERROR)
                tcs.SetException(new Exception());
            else
                tcs.SetResult(output);
        }

        public void RMWCompletionCallback(ref MisterObject key, ref byte[] input, object ctx, Status status)
        {
        }

        public void UpsertCompletionCallback(ref MisterObject key, ref MisterObject value, object ctx)
        {
            if (ctx == null)
                return;

            var tcs = Unsafe.As<IMisterNotifyCompletion>(ctx);
            tcs.SetResult();
        }
    }
}
