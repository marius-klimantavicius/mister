using System.Buffers;
using System.Runtime.CompilerServices;

namespace Marius.Mister
{
    public struct MisterArrayPoolObjectSource : IMisterAtomSource<MisterObject>
    {
        private readonly byte[] _buffer;
        private readonly int _length;

        public MisterArrayPoolObjectSource(byte[] buffer, int length)
        {
            _buffer = buffer;
            _length = length;
        }

        public void Dispose()
        {
            ArrayPool<byte>.Shared.Return(_buffer);
        }

        public ref MisterObject GetAtom()
        {
            ref var value = ref Unsafe.As<byte, MisterObject>(ref _buffer[0]);
            value.Length = _length;
            return ref value;
        }
    }
}
