using System;
using System.Threading.Tasks;

namespace Marius.Mister
{
    public interface IMisterThreadConnection<TKey, TValue>
    {
        void Close();
        void Checkpoint();
        Task CheckpointAsync();
        Task FlushAsync(bool waitPending = false);
        Task<TValue> GetAsync(TKey key, bool waitPending = false);
        Task SetAsync(TKey key, TValue value, bool waitPending = false);
        Task DeleteAsync(TKey key, bool waitPending = false);
        T FlushAsync<T>(T notifyCompletion, bool waitPending = false)
            where T : class, IMisterNotifyCompletion;
        T GetAsync<T>(TKey key, T notifyCompletion, bool waitPending = false)
            where T : class, IMisterNotifyCompletion<TValue>;
        T SetAsync<T>(TKey key, TValue value, T notifyCompletion, bool waitPending = false)
            where T : class, IMisterNotifyCompletion;
        T DeleteAsync<T>(TKey key, T notifyCompletion, bool waitPending = false)
            where T : class, IMisterNotifyCompletion;
        void ForEach(Action<TKey, TValue, bool, object> onRecord, Action<object> onCompleted = null, object state = default(object));
    }
}
