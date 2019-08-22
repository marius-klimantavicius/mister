using System.IO;
using FASTER.core;

namespace Marius.Mister
{
    public interface IMisterConnectionProvider<TKey, TValue, TKeyAtom, TKeyAtomSource, TValueAtom, TValueAtomSource>
        where TKeyAtom : new()
        where TValueAtom : new()
        where TKeyAtomSource : struct, IMisterAtomSource<TKeyAtom>
        where TValueAtomSource : struct, IMisterAtomSource<TValueAtom>
    {
        IMisterSerializer<TKey, TKeyAtom, TKeyAtomSource> KeySerializer { get; }
        IMisterSerializer<TValue, TValueAtom, TValueAtomSource> ValueSerializer { get; }

        IDevice CreateDevice(DirectoryInfo directory);
        IFasterKV<TKeyAtom, TValueAtom, byte[], TValue, object> Create(DirectoryInfo directory, MisterConnectionSettings settings, IDevice mainDevice);
    }
}
