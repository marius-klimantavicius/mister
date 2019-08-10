using System;
using System.Buffers;
using System.IO;

namespace Marius.Mister
{
    public sealed class MisterArrayPoolStreamManager : IMisterStreamManager
    {
        public static readonly IMisterStreamManager Default = new MisterArrayPoolStreamManager(4096);

        private readonly ArrayPool<byte> _arrayPool;
        private readonly int _initialCapacity;

        public MisterArrayPoolStreamManager()
            : this(ArrayPool<byte>.Shared, 0)
        {
        }

        public MisterArrayPoolStreamManager(int initialCapacity)
            : this(ArrayPool<byte>.Shared, initialCapacity)
        {

        }

        public MisterArrayPoolStreamManager(ArrayPool<byte> arrayPool, int initialCapacity)
        {
            if (arrayPool == null)
                throw new ArgumentNullException(nameof(arrayPool));

            _arrayPool = arrayPool;
            _initialCapacity = initialCapacity;
        }

        public MemoryStream GetStream()
        {
            return new MisterArrayPoolStream(_arrayPool, _initialCapacity);
        }
    }
}
