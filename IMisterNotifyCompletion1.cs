using System;

namespace Marius.Mister
{
    public interface IMisterNotifyCompletion<T>
    {
        void SetResult(T value);
        void SetException(Exception ex);
    }
}
