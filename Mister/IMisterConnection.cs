using System;
using System.Threading.Tasks;

namespace Marius.Mister
{
    public interface IMisterConnection<TKey, TValue>
    {
        IMisterSession<TKey, TValue> CreateSession(string sessionId = null);
        
        void Close();
        void Checkpoint();
        Task CheckpointAsync();

        void Flush(bool waitPending = false);
        
        ValueTask<TValue> GetAsync(TKey key, bool waitPending = true);
        ValueTask SetAsync(TKey key, TValue value, bool waitPending = true);
        ValueTask DeleteAsync(TKey key, bool waitPending = true);
        
        void ForEach<TState>(Action<TKey, TValue, bool, TState> onRecord, Action<TState> onCompleted = null, TState state = default(TState));
    }
}
