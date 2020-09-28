using System.IO;

namespace Marius.Mister
{
    public class MisterStreamSerializer<T> : IMisterObjectSerializer<T, MisterStreamObjectSource>
    {
        private readonly IMisterStreamSerializer<T> _streamSerializer;
        private readonly IMisterStreamManager _streamManager;

        public MisterStreamSerializer(IMisterStreamSerializer<T> streamSerializer, IMisterStreamManager streamManager)
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

        public unsafe T Deserialize(ref MisterObject misterObject)
        {
            ref var value = ref misterObject.Data;
            var length = misterObject.Length;

            fixed (byte* data = &value)
            {
                using (var stream = new UnmanagedMemoryStream(data, length))
                    return _streamSerializer.Deserialize(stream);
            }
        }
    }
}
