namespace Marius.Mister
{
    public interface IMisterObjectSerializer<T, TAtomSource> : IMisterSerializer<T, MisterObject, TAtomSource>
        where TAtomSource: struct, IMisterAtomSource<MisterObject>
    {
    }
}
