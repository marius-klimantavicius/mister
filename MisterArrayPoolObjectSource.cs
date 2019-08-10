using System.Buffers;

namespace Marius.Mister
{
    public struct MisterArrayPoolObjectSource : IMisterObjectSource
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

        public ref byte GetObjectBuffer(out int length)
        {
            length = _length;
            return ref _buffer[0];
        }
    }
}
