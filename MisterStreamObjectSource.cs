using System.IO;
using System.Runtime.CompilerServices;

namespace Marius.Mister
{
    public struct MisterStreamObjectSource : IMisterObjectSource
    {
        private MemoryStream _stream;

        public MisterStreamObjectSource(MemoryStream stream)
        {
            _stream = stream;
        }

        public void Dispose()
        {
            _stream.Dispose();
        }

        public ref MisterObject GetObject()
        {
            var buffer = _stream.GetBuffer();

            ref var value = ref Unsafe.As<byte, MisterObject>(ref buffer[0]);
            value.Length = (int)_stream.Length - 4;

            return ref value;
        }
    }
}
