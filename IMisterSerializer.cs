namespace Marius.Mister
{
    public interface IMisterSerializer<T, TObjectSource>
        where TObjectSource: struct, IMisterObjectSource
    {
        TObjectSource Serialize(T value);
        T Deserialize(ref byte value, int length);
    }
}
