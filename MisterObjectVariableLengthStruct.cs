using FASTER.core;

namespace Marius.Mister
{
    public class MisterObjectVariableLengthStruct : IVariableLengthStruct<MisterObject>
    {
        public static MisterObjectVariableLengthStruct Instance = new MisterObjectVariableLengthStruct();

        public int GetAverageLength()
        {
            return 4 * sizeof(int);
        }

        public int GetInitialLength<Input>(ref Input input)
        {
            return 2 * sizeof(int);
        }

        public int GetLength(ref MisterObject t)
        {
            return sizeof(int) + t.Length;
        }
    }
}
