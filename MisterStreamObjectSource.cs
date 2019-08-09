using System.IO;

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

        public ref byte GetObjectBuffer(out int length)
        {
            length = (int)_stream.Length - 4;

            var buffer = _stream.GetBuffer();
            return ref buffer[0];
        }
    }
}
