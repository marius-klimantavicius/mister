using FASTER.core;

namespace Marius.Mister
{
    public unsafe class MisterObjectEqualityComparer : IFasterEqualityComparer<MisterObject>
    {
        public bool Equals(ref MisterObject k1, ref MisterObject k2)
        {
            if (k1.Length != k2.Length)
                return false;

            var length = k1.Length;
            fixed (byte* left = &k1.Data, right = &k2.Data)
            {
                for (var i = 0; i < length; i++)
                    if (left[i] != right[i])
                        return false;
            }

            return true;
        }

        public long GetHashCode64(ref MisterObject k)
        {
            var length = k.Length;
            fixed (byte* ptr = &k.Data)
            {
                return Utility.HashBytes(ptr, length);
            }
        }
    }
}
