namespace Marius.Mister
{
    public interface IMisterSerializer<T, TAtom, TAtomSource>
        where TAtomSource : struct, IMisterAtomSource<TAtom>
    {
        TAtomSource Serialize(T value);
        T Deserialize(ref TAtom value);
    }
}
