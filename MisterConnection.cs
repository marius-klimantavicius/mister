using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;
using log4net;

namespace Marius.Mister
{
    public static class MisterConnection
    {
        public static MisterConnection<TKey, TValue, TKeyAtomSource, TValueAtomSource> Create<TKey, TValue, TKeyAtomSource, TValueAtomSource>(DirectoryInfo directory, IMisterObjectSerializer<TKey, TKeyAtomSource> keySerializer, IMisterObjectSerializer<TValue, TValueAtomSource> valueSerializer, MisterConnectionSettings settings = null, string name = null)
            where TKeyAtomSource : struct, IMisterAtomSource<MisterObject>
            where TValueAtomSource : struct, IMisterAtomSource<MisterObject>
        {
            return new MisterConnection<TKey, TValue, TKeyAtomSource, TValueAtomSource>(directory, keySerializer, valueSerializer, settings, name);
        }

        public static MisterConnection<TKey, TValue, MisterStreamObjectSource, TValueAtomSource> Create<TKey, TValue, TValueAtomSource>(DirectoryInfo directory, IMisterStreamSerializer<TKey> keyStreamSerializer, IMisterObjectSerializer<TValue, TValueAtomSource> valueSerializer, MisterConnectionSettings settings = null, string name = null, IMisterStreamManager streamManager = null)
            where TValueAtomSource : struct, IMisterAtomSource<MisterObject>
        {
            streamManager = streamManager ?? MisterArrayPoolStreamManager.Default;
            var keySerializer = new MisterStreamSerializer<TKey>(keyStreamSerializer, streamManager);

            return new MisterConnection<TKey, TValue, MisterStreamObjectSource, TValueAtomSource>(directory, keySerializer, valueSerializer, settings, name);
        }

        public static MisterConnection<TKey, TValue, TKeyAtomSource, MisterStreamObjectSource> Create<TKey, TValue, TKeyAtomSource>(DirectoryInfo directory, IMisterObjectSerializer<TKey, TKeyAtomSource> keySerializer, IMisterStreamSerializer<TValue> valueStreamSerializer, MisterConnectionSettings settings = null, string name = null, IMisterStreamManager streamManager = null)
            where TKeyAtomSource : struct, IMisterAtomSource<MisterObject>
        {
            streamManager = streamManager ?? MisterArrayPoolStreamManager.Default;
            var valueSerializer = new MisterStreamSerializer<TValue>(valueStreamSerializer, streamManager);

            return new MisterConnection<TKey, TValue, TKeyAtomSource, MisterStreamObjectSource>(directory, keySerializer, valueSerializer, settings, name);
        }

        public static MisterConnection<TKey, TValue> Create<TKey, TValue>(DirectoryInfo directory, IMisterStreamSerializer<TKey> keySerializer, IMisterStreamSerializer<TValue> valueSerializer, MisterConnectionSettings settings = null, string name = null, IMisterStreamManager streamManager = null)
        {
            return new MisterConnection<TKey, TValue>(directory, keySerializer, valueSerializer, settings, name, streamManager);
        }
    }

    public sealed class MisterConnection<TKey, TValue> : IMisterConnection<TKey, TValue>
    {
        private readonly MisterConnection<TKey, TValue, MisterStreamObjectSource, MisterStreamObjectSource> _underlyingConnection;
        private readonly IMisterStreamManager _streamManager;

        public MisterConnection(DirectoryInfo directory, IMisterStreamSerializer<TKey> keySerializer, IMisterStreamSerializer<TValue> valueSerializer, MisterConnectionSettings settings = null, string name = null)
            : this(directory, keySerializer, valueSerializer, settings, name, null)
        {
        }

        public MisterConnection(DirectoryInfo directory, IMisterStreamSerializer<TKey> keySerializer, IMisterStreamSerializer<TValue> valueSerializer, MisterConnectionSettings settings = null, string name = null, IMisterStreamManager streamManager = null)
        {
            if (directory == null)
                throw new ArgumentNullException(nameof(directory));

            if (keySerializer == null)
                throw new ArgumentNullException(nameof(keySerializer));

            if (valueSerializer == null)
                throw new ArgumentNullException(nameof(valueSerializer));

            _streamManager = streamManager ?? MisterArrayPoolStreamManager.Default;

            var streamKeySerializer = new MisterStreamSerializer<TKey>(keySerializer, _streamManager);
            var streamValueSerializer = new MisterStreamSerializer<TValue>(valueSerializer, _streamManager);

            _underlyingConnection = new MisterConnection<TKey, TValue, MisterStreamObjectSource, MisterStreamObjectSource>(directory, streamKeySerializer, streamValueSerializer, settings, name);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Close()
        {
            _underlyingConnection.Close();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Checkpoint()
        {
            _underlyingConnection.Checkpoint();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task CheckpointAsync()
        {
            return _underlyingConnection.CheckpointAsync();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task FlushAsync(bool waitPending)
        {
            return _underlyingConnection.FlushAsync(waitPending);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<TValue> GetAsync(TKey key)
        {
            return _underlyingConnection.GetAsync(key);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<TValue> GetAsync(TKey key, bool waitPending)
        {
            return _underlyingConnection.GetAsync(key, waitPending);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task SetAsync(TKey key, TValue value)
        {
            return _underlyingConnection.SetAsync(key, value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task SetAsync(TKey key, TValue value, bool waitPending)
        {
            return _underlyingConnection.SetAsync(key, value, waitPending);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task DeleteAsync(TKey key)
        {
            return _underlyingConnection.DeleteAsync(key);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task DeleteAsync(TKey key, bool waitPending)
        {
            return _underlyingConnection.DeleteAsync(key, waitPending);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ForEach(Action<TKey, TValue, bool, object> onRecord, Action<object> onCompleted = null, object state = null)
        {
            _underlyingConnection.ForEach(onRecord, onCompleted, state);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T FlushAsync<T>(T notifyCompletion, bool waitPending)
            where T : class, IMisterNotifyCompletion
        {
            return _underlyingConnection.FlushAsync(notifyCompletion, waitPending);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T GetAsync<T>(TKey key, T notifyCompletion, bool waitPending)
            where T : class, IMisterNotifyCompletion<TValue>
        {
            return _underlyingConnection.GetAsync(key, notifyCompletion, waitPending);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T SetAsync<T>(TKey key, TValue value, T notifyCompletion, bool waitPending)
            where T : class, IMisterNotifyCompletion
        {
            return _underlyingConnection.SetAsync(key, value, notifyCompletion, waitPending);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T DeleteAsync<T>(TKey key, T notifyCompletion, bool waitPending)
            where T : class, IMisterNotifyCompletion
        {
            return _underlyingConnection.DeleteAsync(key, notifyCompletion, waitPending);
        }
    }

    public sealed class MisterConnection<TKey, TValue, TKeyAtomSource, TValueAtomSource> : MisterConnection
        <
            TKey,
            TValue,
            MisterObject,
            TKeyAtomSource,
            MisterObject,
            TValueAtomSource,
            MisterObjectEnvironment<TValue, TValueAtomSource>,
            FasterKV<MisterObject, MisterObject, byte[], TValue, object, MisterObjectEnvironment<TValue, TValueAtomSource>>
        >
        where TKeyAtomSource : struct, IMisterAtomSource<MisterObject>
        where TValueAtomSource : struct, IMisterAtomSource<MisterObject>
    {
        public MisterConnection(DirectoryInfo directory, IMisterSerializer<TKey, MisterObject, TKeyAtomSource> keySerializer, IMisterSerializer<TValue, MisterObject, TValueAtomSource> valueSerializer, MisterConnectionSettings settings = null, string name = null)
            : base(directory, keySerializer, valueSerializer, settings, name)
        {
            Initialize();
        }

        protected override void Create()
        {
            if (_faster != null)
                _faster.Dispose();

            if (_mainDevice != null)
                _mainDevice.Close();

            var environment = new MisterObjectEnvironment<TValue, TValueAtomSource>(_valueSerializer);
            var variableLengthStructSettings = new VariableLengthStructSettings<MisterObject, MisterObject>()
            {
                keyLength = MisterObjectVariableLengthStruct.Instance,
                valueLength = MisterObjectVariableLengthStruct.Instance,
            };

            _mainDevice = Devices.CreateLogDevice(Path.Combine(_directory.FullName, @"hlog.log"));
            _faster = new FasterKV<MisterObject, MisterObject, byte[], TValue, object, MisterObjectEnvironment<TValue, TValueAtomSource>>(
                _settings.IndexSize,
                environment,
                _settings.GetLogSettings(_mainDevice),
                new CheckpointSettings() { CheckpointDir = _directory.FullName, CheckPointType = CheckpointType.FoldOver },
                serializerSettings: null,
                comparer: MisterObjectEqualityComparer.Instance,
                variableLengthStructSettings: variableLengthStructSettings
            );
        }
    }

    public abstract class MisterConnection<TKey, TValue, TKeyAtom, TKeyAtomSource, TValueAtom, TValueAtomSource, TFunctions, TFaster> : IMisterConnection<TKey, TValue>
        where TKeyAtom : new()
        where TValueAtom : new()
        where TKeyAtomSource : struct, IMisterAtomSource<TKeyAtom>
        where TValueAtomSource : struct, IMisterAtomSource<TValueAtom>
        where TFunctions: IFunctions<TKeyAtom, TValueAtom, byte[], TValue, object>
        where TFaster : IFasterKV<TKeyAtom, TValueAtom, byte[], TValue, object, TFunctions>
    {
        private static readonly ILog Log = LogManager.GetLogger("MisterConnection");

        private delegate bool MisterWorkAction(ClientSession<TKeyAtom, TValueAtom, byte[], TValue, object, TFunctions> session, ref MisterWorkItem workItem, long sequence);

        private struct MisterWorkItem
        {
            public TKey Key;
            public TValue Value;
            public object State;
            public bool WaitPending;
            public MisterWorkAction Action;
        }

        private class MisterForEachItem
        {
            public Action<TKey, TValue, bool, object> OnRecord;
            public Action<object> OnCompleted;
            public object State;
        }

        private readonly MisterWorkAction _performGet;
        private readonly MisterWorkAction _performSet;
        private readonly MisterWorkAction _performDelete;
        private readonly MisterWorkAction _performAction;
        private readonly MisterWorkAction _performForEach;
        private readonly MisterWorkAction _performFlush;

        protected readonly DirectoryInfo _directory;
        protected readonly IMisterSerializer<TKey, TKeyAtom, TKeyAtomSource> _keySerializer;
        protected readonly IMisterSerializer<TValue, TValueAtom, TValueAtomSource> _valueSerializer;
        protected readonly MisterConnectionSettings _settings;
        protected readonly string _name;
        protected readonly MisterConnectionMaintenanceService<TValue, TKeyAtom, TValueAtom, TFunctions, TFaster> _maintenanceService;

        private readonly CancellationTokenSource _cancellationTokenSource;
        private bool _isClosed;

        private readonly object _lock = new object();

        private ConcurrentQueue<MisterWorkItem> _workQueue;
        private Thread[] _workerThreads;
        private int _sessionsStarted;

        protected TFaster _faster;
        protected IDevice _mainDevice;

        public MisterConnection(DirectoryInfo directory, IMisterSerializer<TKey, TKeyAtom, TKeyAtomSource> keySerializer, IMisterSerializer<TValue, TValueAtom, TValueAtomSource> valueSerializer, MisterConnectionSettings settings = null, string name = null)
        {
            if (directory == null)
                throw new ArgumentNullException(nameof(directory));

            if (keySerializer is null)
                throw new ArgumentNullException(nameof(keySerializer));

            if (valueSerializer is null)
                throw new ArgumentNullException(nameof(valueSerializer));

            _directory = directory;
            _keySerializer = keySerializer;
            _valueSerializer = valueSerializer;
            _settings = settings ?? new MisterConnectionSettings();
            _name = name;
            _cancellationTokenSource = new CancellationTokenSource();

            _maintenanceService = CreateMaintenanceService();

            _performGet = PerformGet;
            _performSet = PerformSet;
            _performDelete = PerformDelete;
            _performAction = PerformAction;
            _performForEach = PerformForEach;
            _performFlush = PerformFlush;
        }

        public void Close()
        {
            if (_isClosed)
                return;

            lock (_lock)
            {
                _isClosed = true;

                _cancellationTokenSource.Cancel();

                lock (_workQueue)
                    Monitor.PulseAll(_workQueue);

                _maintenanceService.Stop();

                for (var i = 0; i < _workerThreads.Length; i++)
                    _workerThreads[i].Join();

                _maintenanceService.Close();

                _faster.Dispose();
                _mainDevice.Close();
                _cancellationTokenSource.Dispose();
            }
        }

        public void Checkpoint()
        {
            CheckDisposed();

            _maintenanceService.Checkpoint();
        }

        public Task CheckpointAsync()
        {
            CheckDisposed();

            return _maintenanceService.CheckpointAsync();
        }

        public async Task FlushAsync(bool waitPending)
        {
            await FlushAsync(new MisterThreadPoolAwaiter(), waitPending);
        }

        public async Task<TValue> GetAsync(TKey key, bool waitPending = false)
        {
            return await GetAsync(key, new MisterThreadPoolAwaiter<TValue>(), waitPending);
        }

        public async Task SetAsync(TKey key, TValue value, bool waitPending = false)
        {
            await SetAsync(key, value, new MisterThreadPoolAwaiter(), waitPending);
        }

        public async Task DeleteAsync(TKey key, bool waitPending = false)
        {
            await DeleteAsync(key, new MisterThreadPoolAwaiter(), waitPending);
        }

        public T FlushAsync<T>(T notifyCompletion, bool waitPending)
            where T : class, IMisterNotifyCompletion
        {
            CheckDisposed();

            _workQueue.Enqueue(new MisterWorkItem()
            {
                Action = _performFlush,
                State = notifyCompletion,
                WaitPending = waitPending,
            });

            lock (_workQueue)
                Monitor.Pulse(_workQueue);

            return notifyCompletion;
        }

        public T GetAsync<T>(TKey key, T notifyCompletion, bool waitPending = false)
            where T : class, IMisterNotifyCompletion<TValue>
        {
            CheckDisposed();

            _workQueue.Enqueue(new MisterWorkItem()
            {
                Key = key,
                Action = _performGet,
                State = notifyCompletion,
                WaitPending = waitPending,
            });

            lock (_workQueue)
                Monitor.Pulse(_workQueue);

            return notifyCompletion;
        }

        public T SetAsync<T>(TKey key, TValue value, T notifyCompletion, bool waitPending = false)
            where T : class, IMisterNotifyCompletion
        {
            CheckDisposed();

            _workQueue.Enqueue(new MisterWorkItem()
            {
                Key = key,
                Value = value,
                Action = _performSet,
                State = notifyCompletion,
            });

            lock (_workQueue)
                Monitor.Pulse(_workQueue);

            return notifyCompletion;
        }

        public T DeleteAsync<T>(TKey key, T notifyCompletion, bool waitPending = false)
            where T : class, IMisterNotifyCompletion
        {
            CheckDisposed();

            _workQueue.Enqueue(new MisterWorkItem()
            {
                Key = key,
                Action = _performDelete,
                State = notifyCompletion,
                WaitPending = waitPending,
            });

            lock (_workQueue)
                Monitor.Pulse(_workQueue);

            return notifyCompletion;
        }

        public void ForEach(Action<TKey, TValue, bool, object> onRecord, Action<object> onCompleted = null, object state = default(object))
        {
            CheckDisposed();

            if (onRecord == null)
                throw new ArgumentNullException(nameof(onRecord));

            _workQueue.Enqueue(new MisterWorkItem()
            {
                Action = _performForEach,
                State = new MisterForEachItem()
                {
                    OnRecord = onRecord,
                    OnCompleted = onCompleted,
                    State = state,
                },
            });

            lock (_workQueue)
                Monitor.Pulse(_workQueue);
        }

        protected abstract void Create();

        protected virtual MisterConnectionMaintenanceService<TValue, TKeyAtom, TValueAtom, TFunctions, TFaster> CreateMaintenanceService()
        {
            return new MisterConnectionMaintenanceService<TValue, TKeyAtom, TValueAtom, TFunctions, TFaster>(_directory, _settings.CheckpointIntervalMilliseconds, _settings.CheckpointCleanCount, _name);
        }

        protected void Initialize()
        {
            _maintenanceService.Recover(() =>
            {
                Create();
                return _faster;
            });

            lock (_lock)
            {
                _workQueue = new ConcurrentQueue<MisterWorkItem>();
                _workerThreads = new Thread[_settings.WorkerThreadCount];
                for (var i = 0; i < _workerThreads.Length; i++)
                    _workerThreads[i] = new Thread(WorkerLoop) { IsBackground = true, Name = $"{_name ?? "Mister"} worker thread #{i + 1}" };

                for (var i = 0; i < _workerThreads.Length; i++)
                    _workerThreads[i].Start(i.ToString());

                while (Volatile.Read(ref _sessionsStarted) < _workerThreads.Length)
                    Thread.Yield();

                _maintenanceService.Start();
            }
        }

        protected void Execute(Action<long> action)
        {
            _workQueue.Enqueue(new MisterWorkItem()
            {
                Action = _performAction,
                State = action,
            });

            lock (_workQueue)
                Monitor.Pulse(_workQueue);
        }

        private unsafe bool PerformGet(ClientSession<TKeyAtom, TValueAtom, byte[], TValue, object, TFunctions> session, ref MisterWorkItem workItem, long sequence)
        {
            var status = default(Status);
            var key = workItem.Key;
            var state = workItem.State;

            try
            {
                var input = default(byte[]);
                var output = default(TValue);

                using (var source = _keySerializer.Serialize(key))
                {
                    ref var misterKey = ref source.GetAtom();

                    status = session.Read(ref misterKey, ref input, ref output, state, sequence);
                    if (status == Status.PENDING)
                    {
                        if (workItem.WaitPending)
                            session.CompletePending(true);
                        return false;
                    }
                }

                if (state != null)
                {
                    var tsc = Unsafe.As<IMisterNotifyCompletion<TValue>>(state);
                    if (status == Status.ERROR)
                        tsc.SetException(new Exception());
                    else
                        tsc.SetResult(output);
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex);

                if (state != null)
                {
                    var tsc = Unsafe.As<IMisterNotifyCompletion<TValue>>(state);
                    tsc.SetException(ex);
                }
            }

            return status != Status.PENDING;
        }

        private unsafe bool PerformSet(ClientSession<TKeyAtom, TValueAtom, byte[], TValue, object, TFunctions> session, ref MisterWorkItem workItem, long sequence)
        {
            var status = default(Status);
            var key = workItem.Key;
            var value = workItem.Value;
            var state = workItem.State;

            try
            {
                using (var keySource = _keySerializer.Serialize(key))
                using (var valueSource = _valueSerializer.Serialize(value))
                {
                    ref var misterKey = ref keySource.GetAtom();
                    ref var misterValue = ref valueSource.GetAtom();

                    status = session.Upsert(ref misterKey, ref misterValue, state, sequence);
                    _maintenanceService.IncrementVersion();

                    if (status == Status.PENDING)
                    {
                        if (workItem.WaitPending)
                            session.CompletePending(true);
                        return false;
                    }
                }

                if (state != null)
                {
                    var tsc = Unsafe.As<IMisterNotifyCompletion>(state);
                    if (status == Status.ERROR)
                        tsc.SetException(new Exception());
                    else
                        tsc.SetResult();
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex);

                if (state != null)
                {
                    var tsc = Unsafe.As<IMisterNotifyCompletion>(state);
                    tsc.SetException(ex);
                }
            }

            return status != Status.PENDING;
        }

        private unsafe bool PerformDelete(ClientSession<TKeyAtom, TValueAtom, byte[], TValue, object, TFunctions> session, ref MisterWorkItem workItem, long sequence)
        {
            var status = default(Status);
            var key = workItem.Key;
            var state = workItem.State;

            try
            {
                using (var keySource = _keySerializer.Serialize(key))
                {
                    ref var misterKey = ref keySource.GetAtom();

                    status = session.Delete(ref misterKey, state, sequence);
                    _maintenanceService.IncrementVersion();

                    if (status == Status.PENDING)
                    {
                        if (workItem.WaitPending)
                            session.CompletePending(true);
                        return false;
                    }
                }

                if (state != null)
                {
                    var tsc = Unsafe.As<IMisterNotifyCompletion>(state);
                    if (status == Status.ERROR)
                        tsc.SetException(new Exception());
                    else
                        tsc.SetResult();
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex);

                if (state != null)
                {
                    var tsc = Unsafe.As<IMisterNotifyCompletion>(state);
                    tsc.SetException(ex);
                }
            }

            return status != Status.PENDING;
        }

        private bool PerformFlush(ClientSession<TKeyAtom, TValueAtom, byte[], TValue, object, TFunctions> session, ref MisterWorkItem workItem, long sequence)
        {
            var state = workItem.State;
            var wait = workItem.WaitPending;

            try
            {
                _faster.Log.FlushAndEvict(wait);
                _maintenanceService.IncrementVersion();

                if (state != null)
                {
                    var tsc = Unsafe.As<IMisterNotifyCompletion>(state);
                    tsc.SetResult();
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex);

                if (state != null)
                {
                    var tsc = Unsafe.As<IMisterNotifyCompletion>(state);
                    tsc.SetException(ex);
                }
            }

            return true;
        }

        private unsafe bool PerformForEach(ClientSession<TKeyAtom, TValueAtom, byte[], TValue, object, TFunctions> session, ref MisterWorkItem workItem, long sequence)
        {
            var forEachItem = Unsafe.As<MisterForEachItem>(workItem.State);
            var iterator = _faster.Log.Scan(_faster.Log.BeginAddress, _faster.Log.TailAddress);
            while (iterator.GetNext(out var recordInfo))
            {
                ref var misterKey = ref iterator.GetKey();
                ref var misterValue = ref iterator.GetValue();

                var key = _keySerializer.Deserialize(ref misterKey);
                var value = _valueSerializer.Deserialize(ref misterValue);

                var isDeleted = recordInfo.Tombstone;
                forEachItem.OnRecord(key, value, isDeleted, forEachItem.State);
            }

            if (forEachItem.OnCompleted != null)
                forEachItem.OnCompleted(forEachItem.State);

            return true;
        }

        private bool PerformAction(ClientSession<TKeyAtom, TValueAtom, byte[], TValue, object, TFunctions> session, ref MisterWorkItem workItem, long sequence)
        {
            try
            {
                var action = (Action<long>)(workItem.State);
                action(sequence);
            }
            catch { }

            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckDisposed()
        {
            if (_isClosed)
                throw new ObjectDisposedException("MisterConnection");
        }

        private void WorkerLoop(object state)
        {
            try
            {
                var workerRefreshIntervalMilliseconds = _settings.WorkerRefreshIntervalMilliseconds;
                var sequence = 0L;
                using (var session = _faster.NewSession(threadAffinitized: true))
                {
                    var hasPending = false;
                    Interlocked.Increment(ref _sessionsStarted);

                    while (!_cancellationTokenSource.IsCancellationRequested)
                    {
                        var needRefresh = false;
                        if (_workQueue.IsEmpty)
                        {
                            lock (_workQueue)
                            {
                                if (_workQueue.IsEmpty && !_cancellationTokenSource.IsCancellationRequested)
                                    needRefresh = !Monitor.Wait(_workQueue, workerRefreshIntervalMilliseconds);
                            }
                        }

                        while (_workQueue.TryDequeue(out var item))
                        {
                            needRefresh = false;

                            if (!item.Action(session, ref item, sequence))
                                hasPending = true;

                            sequence++;
                            if ((sequence & 0x7F) == 0)
                            {
                                if (hasPending)
                                {
                                    session.CompletePending(true);
                                    hasPending = false;
                                }

                                session.Refresh();
                            }
                        }

                        if (hasPending)
                        {
                            session.CompletePending(true);
                            hasPending = false;
                        }

                        if (needRefresh)
                        {
                            session.CompletePending(false);
                            session.Refresh();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex);
                Trace.Write(ex);
            }
        }
    }
}
