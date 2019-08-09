using System.IO;
using System.Text;

namespace Marius.Mister
{
    public class MisterStreamStringSerializer : IMisterStreamSerializer<string>
    {
        public string Deserialize(Stream stream)
        {
            using (var reader = new StreamReader(stream, Encoding.UTF8, false, 128, true))
                return reader.ReadToEnd();
        }

        public void Serialize(Stream stream, string value)
        {
            using (var writer = new StreamWriter(stream, Encoding.UTF8, 128, true))
                writer.Write(value);
        }
    }
}
