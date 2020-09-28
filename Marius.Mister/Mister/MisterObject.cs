using System.Runtime.InteropServices;

namespace Marius.Mister
{
    [StructLayout(LayoutKind.Explicit)]
    public struct MisterObject
    {
        [FieldOffset(0)]
        public int Length;

        [FieldOffset(4)]
        public byte Data;
    }
}
