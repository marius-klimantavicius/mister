using System;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;

namespace Marius.Mister
{
    public sealed class MisterThreadPoolAwaiter : IMisterNotifyCompletion, ICriticalNotifyCompletion
    {
        private static readonly Action _completed = () => { };
        private static readonly WaitCallback _execute = s =>
        {
            var continuation = Unsafe.As<Action>(s);
            continuation();
        };

        private Action _continuation;
        private ExceptionDispatchInfo _exception;

        public bool IsCompleted { get { return ReferenceEquals(_completed, Volatile.Read(ref _continuation)); } }

        public MisterThreadPoolAwaiter GetAwaiter()
        {
            return this;
        }

        public void GetResult()
        {
            if (_exception != null)
                _exception.Throw();
        }

        public void OnCompleted(Action continuation)
        {
            if (continuation == null)
                return;

            var old = Interlocked.CompareExchange(ref _continuation, continuation, null);
            if (ReferenceEquals(old, _completed))
                ThreadPool.UnsafeQueueUserWorkItem(_execute, continuation);
            else if (old != null)
                throw new NotSupportedException();
        }

        public void UnsafeOnCompleted(Action continuation)
        {
            OnCompleted(continuation);
        }

        public void SetException(Exception ex)
        {
            _exception = ExceptionDispatchInfo.Capture(ex);
            SetResult();
        }

        public void SetResult()
        {
            var continuation = Interlocked.Exchange(ref _continuation, _completed);
            if (continuation != null && !ReferenceEquals(continuation, _completed))
                ThreadPool.UnsafeQueueUserWorkItem(_execute, continuation);
        }
    }

    public sealed class MisterThreadPoolAwaiter<T> : ICriticalNotifyCompletion, IMisterNotifyCompletion<T>
    {
        private static readonly Action _completed = () => { };
        private static readonly WaitCallback _execute = s =>
        {
            var continuation = Unsafe.As<Action>(s);
            continuation();
        };

        private Action _continuation;

        private T _result;
        private ExceptionDispatchInfo _exception;

        public bool IsCompleted { get { return ReferenceEquals(_completed, Volatile.Read(ref _continuation)); } }

        public MisterThreadPoolAwaiter<T> GetAwaiter()
        {
            return this;
        }

        public T GetResult()
        {
            if (_exception != null)
                _exception.Throw();

            return _result;
        }

        public void OnCompleted(Action continuation)
        {
            if (continuation == null)
                return;

            var old = Interlocked.CompareExchange(ref _continuation, continuation, null);
            if (ReferenceEquals(old, _completed))
                ThreadPool.UnsafeQueueUserWorkItem(_execute, continuation);
            else if (old != null)
                throw new NotSupportedException();
        }

        public void UnsafeOnCompleted(Action continuation)
        {
            OnCompleted(continuation);
        }

        public void SetException(Exception ex)
        {
            _exception = ExceptionDispatchInfo.Capture(ex);
            Complete();
        }

        public void SetResult(T value)
        {
            _result = value;
            Complete();
        }

        private void Complete()
        {
            var continuation = Interlocked.Exchange(ref _continuation, _completed);
            if (continuation != null && !ReferenceEquals(continuation, _completed))
                ThreadPool.UnsafeQueueUserWorkItem(_execute, continuation);
        }
    }
}
