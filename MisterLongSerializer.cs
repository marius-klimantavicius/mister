using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Marius.Mister
{
    public unsafe class MisterLongSerializer : IMisterSerializer<long>
    {
        [ThreadStatic]
        private static byte[] _threadBuffer;

        public long Deserialize(Stream stream)
        {
            if (_threadBuffer == null)
                _threadBuffer = new byte[8];

            var read = 0;
            var offset = 0;
            var length = 8;
            do
            {
                read = stream.Read(_threadBuffer, offset, length);
                offset += read;
                length -= read;
            } while (read > 0);

            fixed (byte* ptr = _threadBuffer)
                return *(long*)ptr;
        }

        public void Serialize(Stream stream, long value)
        {
            if (_threadBuffer == null)
                _threadBuffer = new byte[8];

            fixed (byte* ptr = _threadBuffer)
                *(long*)ptr = value;

            stream.Write(_threadBuffer, 0, _threadBuffer.Length);
        }
    }
}
