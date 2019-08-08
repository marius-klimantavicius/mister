using System.IO;

namespace Marius.Mister
{
    public interface IMisterStreamSerializer<T>
    {
        void Serialize(Stream stream, T value);
        T Deserialize(Stream stream);
    }
}
