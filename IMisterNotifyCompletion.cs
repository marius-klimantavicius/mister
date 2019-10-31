using System;

namespace Marius.Mister
{
    public interface IMisterNotifyCompletion
    {
        void SetResult();
        void SetException(Exception ex);
    }
}
