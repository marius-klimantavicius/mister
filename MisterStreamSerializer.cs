using System.IO;
using Microsoft.IO;

namespace Marius.Mister
{
    public class MisterStreamSerializer<T> : IMisterSerializer<T, MisterStreamObjectSource>
    {
        private readonly IMisterStreamSerializer<T> _streamSerializer;
        private readonly RecyclableMemoryStreamManager _streamManager;

        public MisterStreamSerializer(IMisterStreamSerializer<T> streamSerializer, RecyclableMemoryStreamManager streamManager)
        {
            _streamSerializer = streamSerializer;
            _streamManager = streamManager;
        }

        public MisterStreamObjectSource Serialize(T value)
        {
            var stream = _streamManager.GetStream();
            stream.Position = 4;
            try
            {
                _streamSerializer.Serialize(stream, value);
                return new MisterStreamObjectSource(stream);
            }
            catch
            {
                stream.Dispose();
                throw;
            }
        }

        public unsafe T Deserialize(ref byte value, int length)
        {
            fixed (byte* data = &value)
            {
                using (var stream = new UnmanagedMemoryStream(data, length))
                    return _streamSerializer.Deserialize(stream);
            }
        }
    }
}
