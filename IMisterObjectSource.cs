using System;

namespace Marius.Mister
{
    public interface IMisterObjectSource : IDisposable
    {
        ref MisterObject GetObject();
    }
}
