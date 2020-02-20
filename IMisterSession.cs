using System;
using System.Threading.Tasks;

namespace Marius.Mister
{
    public interface IMisterSession<TKey, TValue> : IDisposable
    {
        ValueTask<TValue> GetAsync(TKey key, bool waitPending = false);
        ValueTask SetAsync(TKey key, TValue value, bool waitPending = false);
        ValueTask DeleteAsync(TKey key, bool waitPending = false);
    }
}
