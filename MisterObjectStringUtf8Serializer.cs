using System.Buffers;
using System.Text;

namespace Marius.Mister
{
    public unsafe class MisterObjectStringUtf8Serializer : IMisterObjectSerializer<string, MisterArrayPoolObjectSource>
    {
        public MisterArrayPoolObjectSource Serialize(string value)
        {
            var bufferLength = Encoding.UTF8.GetByteCount(value);
            var buffer = ArrayPool<byte>.Shared.Rent(bufferLength + 4);

            bufferLength = Encoding.UTF8.GetBytes(value, 0, value.Length, buffer, 4);

            return new MisterArrayPoolObjectSource(buffer, bufferLength);
        }

        public string Deserialize(ref MisterObject misterObject)
        {
            ref var value = ref misterObject.Data;
            var length = misterObject.Length;

            fixed (byte* ptr = &value)
                return Encoding.UTF8.GetString(ptr, length);
        }
    }
}
