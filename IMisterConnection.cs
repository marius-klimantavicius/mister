using System;
using System.Threading.Tasks;

namespace Marius.Mister
{
    public interface IMisterConnection<TKey, TValue>
    {
        void Close();
        void Checkpoint();
        Task FlushAsync(bool waitPending);
        Task CheckpointAsync();
        Task<TValue> GetAsync(TKey key);
        Task<TValue> GetAsync(TKey key, bool waitPending);
        Task SetAsync(TKey key, TValue value);
        Task SetAsync(TKey key, TValue value, bool waitPending);
        Task DeleteAsync(TKey key);
        Task DeleteAsync(TKey key, bool waitPending);
        void ForEach(Action<TKey, TValue, bool, object> onRecord, Action<object> onCompleted = null, object state = default(object));
        Task CompactAsync();
    }
}
