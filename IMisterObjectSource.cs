using System;

namespace Marius.Mister
{
    public interface IMisterObjectSource : IDisposable
    {
        /// <summary>
        /// Must point to start of the buffer:
        ///     4-bytes reserved for length
        ///     {length}-bytes - actual serialized value
        /// Must be valid until the scope is disposed
        /// </summary>
        /// <returns></returns>
        ref byte GetObjectBuffer(out int length);
    }
}
