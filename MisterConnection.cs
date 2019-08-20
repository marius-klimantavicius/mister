using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
        public static MisterConnection<TKey, TValue, TKeyObjectSource, TValueObjectSource> Create<TKey, TValue, TKeyObjectSource, TValueObjectSource>(DirectoryInfo directory, IMisterSerializer<TKey, TKeyObjectSource> keySerializer, IMisterSerializer<TValue, TValueObjectSource> valueSerializer, MisterConnectionSettings settings = null, string name = null)
            where TKeyObjectSource : struct, IMisterObjectSource
            where TValueObjectSource : struct, IMisterObjectSource
        {
            return new MisterConnection<TKey, TValue, TKeyObjectSource, TValueObjectSource>(directory, keySerializer, valueSerializer, settings, name);
        }

        public static MisterConnection<TKey, TValue, MisterStreamObjectSource, TValueObjectSource> Create<TKey, TValue, TValueObjectSource>(DirectoryInfo directory, IMisterStreamSerializer<TKey> keyStreamSerializer, IMisterSerializer<TValue, TValueObjectSource> valueSerializer, MisterConnectionSettings settings = null, string name = null, IMisterStreamManager streamManager = null)
            where TValueObjectSource : struct, IMisterObjectSource
        {
            streamManager = streamManager ?? MisterArrayPoolStreamManager.Default;
            var keySerializer = new MisterStreamSerializer<TKey>(keyStreamSerializer, streamManager);

            return new MisterConnection<TKey, TValue, MisterStreamObjectSource, TValueObjectSource>(directory, keySerializer, valueSerializer, settings, name);
        }

        public static MisterConnection<TKey, TValue, TKeyObjectSource, MisterStreamObjectSource> Create<TKey, TValue, TKeyObjectSource>(DirectoryInfo directory, IMisterSerializer<TKey, TKeyObjectSource> keySerializer, IMisterStreamSerializer<TValue> valueStreamSerializer, MisterConnectionSettings settings = null, string name = null, IMisterStreamManager streamManager = null)
            where TKeyObjectSource : struct, IMisterObjectSource
        {
            streamManager = streamManager ?? MisterArrayPoolStreamManager.Default;
            var valueSerializer = new MisterStreamSerializer<TValue>(valueStreamSerializer, streamManager);

            return new MisterConnection<TKey, TValue, TKeyObjectSource, MisterStreamObjectSource>(directory, keySerializer, valueSerializer, settings, name);
        }

        public static MisterConnection<TKey, TValue> Create<TKey, TValue>(DirectoryInfo directory, IMisterStreamSerializer<TKey> keySerializer, IMisterStreamSerializer<TValue> valueSerializer, MisterConnectionSettings settings = null, string name = null, IMisterStreamManager streamManager = null)
        {
            return new MisterConnection<TKey, TValue>(directory, keySerializer, valueSerializer, settings, name, streamManager);
        }
    }

    public sealed class MisterConnection<TKey, TValue>
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
        public Task<TValue> GetAsync(TKey key)
        {
            return _underlyingConnection.GetAsync(key);
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
    }

    public sealed class MisterConnection<TKey, TValue, TKeyObjectSource, TValueObjectSource>
        where TKeyObjectSource : struct, IMisterObjectSource
        where TValueObjectSource : struct, IMisterObjectSource
    {
        private static readonly ILog Log = LogManager.GetLogger("MisterConnection");

        private delegate bool MisterWorkAction(ref MisterWorkItem workItem, long sequence);

        private struct MisterWorkItem
        {
            public TKey Key;
            public TValue Value;
            public object State;
            public bool WaitPending;
            public MisterWorkAction Action;
        }

        private struct MisterCheckpointItem
        {
            public AutoResetEvent ResetEvent;
            public TaskCompletionSource<MisterVoid> TaskCompletionSource;

            public MisterCheckpointItem(AutoResetEvent resetEvent)
            {
                ResetEvent = resetEvent;
                TaskCompletionSource = null;
            }

            public MisterCheckpointItem(TaskCompletionSource<MisterVoid> taskCompletionSource)
            {
                TaskCompletionSource = taskCompletionSource;
                ResetEvent = null;
            }
        }

        private class MisterForEachItem
        {
            public Action<TKey, TValue, bool, object> OnRecord;
            public Action<object> OnCompleted;
            public object State;
        }

        private readonly DirectoryInfo _directory;
        private readonly IMisterSerializer<TKey, TKeyObjectSource> _keySerializer;
        private readonly IMisterSerializer<TValue, TValueObjectSource> _valueSerializer;
        private readonly MisterConnectionSettings _settings;
        private readonly string _name;
        private readonly CancellationTokenSource _cancellationTokenSource;

        private FasterKV<MisterObject, MisterObject, byte[], TValue, object, MisterObjectEnvironment<TValue, TValueObjectSource>> _faster;
        private IDevice _mainLog;

        private ConcurrentQueue<MisterWorkItem> _workQueue;
        private Thread[] _workerThreads;

        private ConcurrentQueue<MisterCheckpointItem> _checkpointQueue;
        private Thread _checkpointThread;

        private int _checkpointVersion;
        private int _checkpointRunning;
        private SpinLock _spinLock = new SpinLock();

        private bool _isClosed;

        public MisterConnection(DirectoryInfo directory, IMisterSerializer<TKey, TKeyObjectSource> keySerializer, IMisterSerializer<TValue, TValueObjectSource> valueSerializer, MisterConnectionSettings settings = null, string name = null)
        {
            if (directory == null)
                throw new ArgumentNullException(nameof(directory));

            if (keySerializer == null)
                throw new ArgumentNullException(nameof(keySerializer));

            if (valueSerializer == null)
                throw new ArgumentNullException(nameof(valueSerializer));

            _directory = directory;
            _keySerializer = keySerializer;
            _valueSerializer = valueSerializer;
            _settings = settings ?? new MisterConnectionSettings();
            _name = name;
            _cancellationTokenSource = new CancellationTokenSource();

            Initialize();
        }

        public void Close()
        {
            if (_isClosed)
                return;

            _isClosed = true;

            PerformCheckpoint();

            _cancellationTokenSource.Cancel();

            lock (_workQueue)
                Monitor.PulseAll(_workQueue);

            lock (_checkpointQueue)
                Monitor.PulseAll(_checkpointQueue);

            for (var i = 0; i < _workerThreads.Length; i++)
                _workerThreads[i].Join();

            _checkpointThread.Join();

            _faster.Dispose();
            _mainLog.Close();
            _cancellationTokenSource.Dispose();
        }

        public void Checkpoint()
        {
            CheckDisposed();

            PerformCheckpoint();
        }

        public Task FlushAsync(bool waitPending)
        {
            CheckDisposed();

            var tsc = new TaskCompletionSource<MisterVoid>(TaskCreationOptions.RunContinuationsAsynchronously);
            _workQueue.Enqueue(new MisterWorkItem()
            {
                Action = PerformFlush,
                State = tsc,
                WaitPending = waitPending,
            });

            lock (_workQueue)
                Monitor.Pulse(_workQueue);

            return tsc.Task;
        }

        public Task CheckpointAsync()
        {
            CheckDisposed();

            return PerformCheckpointAsync();
        }

        public Task<TValue> GetAsync(TKey key)
        {
            CheckDisposed();

            var tsc = new TaskCompletionSource<TValue>(TaskCreationOptions.RunContinuationsAsynchronously);
            _workQueue.Enqueue(new MisterWorkItem()
            {
                Key = key,
                State = tsc,
                Action = PerformGet,
            });

            lock (_workQueue)
                Monitor.Pulse(_workQueue);

            return tsc.Task;
        }

        public Task<TValue> GetAsync(TKey key, bool waitPending)
        {
            CheckDisposed();

            var tsc = new TaskCompletionSource<TValue>(TaskCreationOptions.RunContinuationsAsynchronously);
            _workQueue.Enqueue(new MisterWorkItem()
            {
                Key = key,
                State = tsc,
                Action = PerformGet,
                WaitPending = waitPending,
            });

            lock (_workQueue)
                Monitor.Pulse(_workQueue);

            return tsc.Task;
        }

        public Task SetAsync(TKey key, TValue value)
        {
            CheckDisposed();

            var tsc = new TaskCompletionSource<MisterVoid>(TaskCreationOptions.RunContinuationsAsynchronously);
            _workQueue.Enqueue(new MisterWorkItem()
            {
                Key = key,
                Value = value,
                State = tsc,
                Action = PerformSet,
            });

            lock (_workQueue)
                Monitor.Pulse(_workQueue);

            return tsc.Task;
        }

        public Task SetAsync(TKey key, TValue value, bool waitPending)
        {
            CheckDisposed();

            var tsc = new TaskCompletionSource<MisterVoid>(TaskCreationOptions.RunContinuationsAsynchronously);
            _workQueue.Enqueue(new MisterWorkItem()
            {
                Key = key,
                Value = value,
                State = tsc,
                WaitPending = waitPending,
                Action = PerformSet,
            });

            lock (_workQueue)
                Monitor.Pulse(_workQueue);

            return tsc.Task;
        }

        public Task DeleteAsync(TKey key)
        {
            CheckDisposed();

            var tsc = new TaskCompletionSource<MisterVoid>(TaskCreationOptions.RunContinuationsAsynchronously);
            _workQueue.Enqueue(new MisterWorkItem()
            {
                Key = key,
                State = tsc,
                Action = PerformDelete,
            });

            lock (_workQueue)
                Monitor.Pulse(_workQueue);

            return tsc.Task;
        }

        public Task DeleteAsync(TKey key, bool waitPending)
        {
            CheckDisposed();

            var tsc = new TaskCompletionSource<MisterVoid>(TaskCreationOptions.RunContinuationsAsynchronously);
            _workQueue.Enqueue(new MisterWorkItem()
            {
                Key = key,
                State = tsc,
                WaitPending = waitPending,
                Action = PerformDelete,
            });

            lock (_workQueue)
                Monitor.Pulse(_workQueue);

            return tsc.Task;
        }

        public void ForEach(Action<TKey, TValue, bool, object> onRecord, Action<object> onCompleted = null, object state = default(object))
        {
            if (onRecord == null)
                throw new ArgumentNullException(nameof(onRecord));

            _workQueue.Enqueue(new MisterWorkItem()
            {
                Action = PerformForEach,
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

        public Task CompactAsync()
        {
            var tcs = new TaskCompletionSource<MisterVoid>();
            _workQueue.Enqueue(new MisterWorkItem()
            {
                Action = PerformCompact,
                State = tcs,
            });

            lock (_workQueue)
                Monitor.Pulse(_workQueue);

            return tcs.Task;
        }

        private unsafe bool PerformGet(ref MisterWorkItem workItem, long sequence)
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
                    ref var keyBuffer = ref source.GetObjectBuffer(out var length);
                    fixed (byte* keyPointer = &keyBuffer)
                    {
                        ref var misterKey = ref *(MisterObject*)keyPointer;
                        misterKey.Length = length;

                        status = _faster.Read(ref misterKey, ref input, ref output, state, sequence);
                        if (status == Status.PENDING)
                        {
                            if (workItem.WaitPending)
                                _faster.CompletePending(true);
                            return false;
                        }
                    }
                }

                if (state != null)
                {
                    var tsc = Unsafe.As<TaskCompletionSource<TValue>>(state);
                    if (status == Status.ERROR)
                        tsc.SetException(new Exception());
                    else
                        tsc.SetResult(output);
                }
            }
            catch (Exception ex)
            {
                if (state != null)
                {
                    var tsc = (TaskCompletionSource<TValue>)state;
                    tsc.SetException(ex);
                }
            }

            return status != Status.PENDING;
        }

        private unsafe bool PerformSet(ref MisterWorkItem workItem, long sequence)
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
                    ref var keyBuffer = ref keySource.GetObjectBuffer(out var keyLength);
                    ref var valueBuffer = ref valueSource.GetObjectBuffer(out var valueLength);
                    fixed (byte* keyPointer = &keyBuffer, valuePointer = &valueBuffer)
                    {
                        ref var misterKey = ref *(MisterObject*)keyPointer;
                        ref var misterValue = ref *(MisterObject*)valuePointer;

                        misterKey.Length = keyLength;
                        misterValue.Length = valueLength;

                        status = _faster.Upsert(ref misterKey, ref misterValue, state, sequence);
                        Interlocked.Increment(ref _checkpointVersion);

                        if (status == Status.PENDING)
                        {
                            if (workItem.WaitPending)
                                _faster.CompletePending(true);
                            return false;
                        }
                    }
                }

                if (state != null)
                {
                    var tsc = Unsafe.As<TaskCompletionSource<MisterVoid>>(state);
                    if (status == Status.ERROR)
                        tsc.SetException(new Exception());
                    else
                        tsc.SetResult(MisterVoid.Value);
                }
            }
            catch (Exception ex)
            {
                if (state != null)
                {
                    var tsc = (TaskCompletionSource<MisterVoid>)state;
                    tsc.SetException(ex);
                }
            }

            return status != Status.PENDING;
        }

        private unsafe bool PerformDelete(ref MisterWorkItem workItem, long sequence)
        {
            var status = default(Status);
            var key = workItem.Key;
            var state = workItem.State;

            try
            {
                using (var keySource = _keySerializer.Serialize(key))
                {
                    ref var buffer = ref keySource.GetObjectBuffer(out var length);
                    fixed (byte* keyPointer = &buffer)
                    {
                        ref var misterKey = ref *(MisterObject*)keyPointer;
                        misterKey.Length = length;

                        status = _faster.Delete(ref misterKey, state, sequence);
                        Interlocked.Increment(ref _checkpointVersion);

                        if (status == Status.PENDING)
                        {
                            if (workItem.WaitPending)
                                _faster.CompletePending(true);
                            return false;
                        }
                    }
                }

                if (state != null)
                {
                    var tsc = Unsafe.As<TaskCompletionSource<MisterVoid>>(state);
                    if (status == Status.ERROR)
                        tsc.SetException(new Exception());
                    else
                        tsc.SetResult(MisterVoid.Value);
                }
            }
            catch (Exception ex)
            {
                if (state != null)
                {
                    var tsc = (TaskCompletionSource<MisterVoid>)state;
                    tsc.SetException(ex);
                }
            }

            return status != Status.PENDING;
        }

        private bool PerformFlush(ref MisterWorkItem workItem, long sequence)
        {
            var state = workItem.State;
            var wait = workItem.WaitPending;

            try
            {
                _faster.Log.FlushAndEvict(wait);
                Interlocked.Increment(ref _checkpointVersion);

                if (state != null)
                {
                    var tsc = Unsafe.As<TaskCompletionSource<MisterVoid>>(state);
                    tsc.SetResult(MisterVoid.Value);
                }
            }
            catch (Exception ex)
            {
                if (state != null)
                {
                    var tsc = (TaskCompletionSource<MisterVoid>)state;
                    tsc.SetException(ex);
                }
            }

            return true;
        }

        private unsafe bool PerformForEach(ref MisterWorkItem workItem, long sequence)
        {
            var forEachItem = Unsafe.As<MisterForEachItem>(workItem.State);
            var iterator = _faster.Log.Scan(_faster.Log.BeginAddress, _faster.Log.TailAddress);
            while (iterator.GetNext(out var recordInfo))
            {
                ref var misterKey = ref iterator.GetKey();
                ref var misterValue = ref iterator.GetValue();

                var key = _keySerializer.Deserialize(ref misterKey.Data, misterKey.Length);
                var value = _valueSerializer.Deserialize(ref misterValue.Data, misterValue.Length);

                var isDeleted = recordInfo.Tombstone;
                forEachItem.OnRecord(key, value, isDeleted, forEachItem.State);
            }

            if (forEachItem.OnCompleted != null)
                forEachItem.OnCompleted(forEachItem.State);
            return true;
        }

        private bool PerformCompact(ref MisterWorkItem workItem, long sequence)
        {
            var state = workItem.State;
            try
            {
                _faster.Log.Compact(_faster.Log.SafeReadOnlyAddress);
                Interlocked.Increment(ref _checkpointVersion);

                if (state != null)
                {
                    var tsc = Unsafe.As<TaskCompletionSource<MisterVoid>>(state);
                    tsc.SetResult(MisterVoid.Value);
                }
            }
            catch (Exception ex)
            {
                if (state != null)
                {
                    var tsc = (TaskCompletionSource<MisterVoid>)state;
                    tsc.SetException(ex);
                }
            }

            return false;
        }

        private void PerformCheckpoint()
        {
            using (var handle = new AutoResetEvent(false))
            {
                _checkpointQueue.Enqueue(new MisterCheckpointItem(handle));

                lock (_checkpointQueue)
                    Monitor.Pulse(_checkpointQueue);

                handle.WaitOne();
            }
        }

        private Task PerformCheckpointAsync()
        {
            var tsc = new TaskCompletionSource<MisterVoid>(TaskCreationOptions.RunContinuationsAsynchronously);
            _checkpointQueue.Enqueue(new MisterCheckpointItem(tsc));

            lock (_checkpointQueue)
                Monitor.Pulse(_checkpointQueue);

            return tsc.Task;
        }

        private void Initialize()
        {
            var checkpointTokenFile = new FileInfo(Path.Combine(_directory.FullName, "checkpoint_token.txt"));
            var checkpointTokenBackupFile = new FileInfo(Path.Combine(_directory.FullName, "checkpoint_token_backup.txt"));

            CleanOldCheckpoints(checkpointTokenFile, checkpointTokenBackupFile);

            if (!TryRecover(checkpointTokenFile))
            {
                if (!TryRecover(checkpointTokenBackupFile))
                    Create();
            }

            _workQueue = new ConcurrentQueue<MisterWorkItem>();
            _workerThreads = new Thread[_settings.WorkerThreadCount];
            for (var i = 0; i < _workerThreads.Length; i++)
                _workerThreads[i] = new Thread(WorkerLoop) { IsBackground = true, Name = $"{_name ?? "Mister"} worker thread #{i + 1}" };

            _checkpointQueue = new ConcurrentQueue<MisterCheckpointItem>();
            _checkpointThread = new Thread(CheckpointLoop) { IsBackground = true, Name = $"{_name ?? "Mister"} checkpoint thread" };

            for (var i = 0; i < _workerThreads.Length; i++)
                _workerThreads[i].Start(i.ToString());

            _checkpointThread.Start();
        }

        private void Create()
        {
            if (_faster != null)
                _faster.Dispose();

            if (_mainLog != null)
                _mainLog.Close();

            var environment = new MisterObjectEnvironment<TValue, TValueObjectSource>(_valueSerializer);
            var variableLengthStructSettings = new VariableLengthStructSettings<MisterObject, MisterObject, byte[]>()
            {
                keyLength = new MisterObjectVariableLengthStruct(),
                valueLength = new MisterObjectVariableLengthStruct(),
                functions = environment,
            };

            _mainLog = Devices.CreateLogDevice(Path.Combine(_directory.FullName, @"hlog.log"));
            var logSettings = new LogSettings
            {
                LogDevice = _mainLog,
            };
            _settings.Apply(logSettings);

            _faster = new FasterKV<MisterObject, MisterObject, byte[], TValue, object, MisterObjectEnvironment<TValue, TValueObjectSource>>(
                _settings.IndexSize,
                new MisterObjectEnvironment<TValue, TValueObjectSource>(_valueSerializer),
                logSettings,
                new CheckpointSettings() { CheckpointDir = _directory.FullName, CheckPointType = CheckpointType.FoldOver },
                serializerSettings: null,
                comparer: new MisterObjectEqualityComparer(),
                variableLengthStructSettings: variableLengthStructSettings
            );
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckDisposed()
        {
            if (_isClosed)
                throw new ObjectDisposedException("MisterConnection");
        }

        private bool TryRecover(FileInfo checkpointTokenFile)
        {
            if (checkpointTokenFile.Exists)
            {
                try
                {
                    var checkpointToken = default(Guid?);
                    using (var reader = checkpointTokenFile.OpenText())
                    {
                        var line = reader.ReadLine();
                        if (Guid.TryParse(line, out var result))
                            checkpointToken = result;
                    }

                    if (checkpointToken != null)
                    {
                        Create();
                        _faster.Recover(checkpointToken.Value);
                    }
                    else
                    {
                        checkpointTokenFile.Delete();
                    }

                    return checkpointToken != null;
                }
                catch (Exception ex)
                {
                    Log.Error(ex);
                }
            }

            return false;
        }

        private void CleanOldCheckpoints(FileInfo checkpointTokenFile, FileInfo checkpointTokenBackup)
        {
            var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            if (checkpointTokenFile.Exists)
            {
                using (var reader = checkpointTokenFile.OpenText())
                {
                    var line = reader.ReadLine();
                    if (Guid.TryParse(line, out var result))
                        set.Add(result.ToString());
                }
            }

            if (checkpointTokenBackup.Exists)
            {
                using (var reader = checkpointTokenBackup.OpenText())
                {
                    var line = reader.ReadLine();
                    if (Guid.TryParse(line, out var result))
                        set.Add(result.ToString());
                }
            }

            try
            {
                var info = new DirectoryInfo(Path.Combine(_directory.FullName, "cpr-checkpoints"));
                foreach (var item in info.EnumerateDirectories())
                {
                    if (set.Contains(item.Name))
                        continue;

                    try
                    {
                        item.Delete(true);
                    }
                    catch { }
                }
            }
            catch { }

            try
            {
                var info = new DirectoryInfo(Path.Combine(_directory.FullName, "index-checkpoints"));
                foreach (var item in info.EnumerateDirectories())
                {
                    if (set.Contains(item.Name))
                        continue;

                    try
                    {
                        item.Delete(true);
                    }
                    catch { }
                }
            }
            catch { }
        }

        private void WorkerLoop(object state)
        {
            try
            {
                var workerRefreshIntervalMilliseconds = _settings.WorkerRefreshIntervalMilliseconds;
                var sequence = 0L;
                var session = _faster.StartSession();
                var hasPending = false;

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

                    while (_workQueue.TryDequeue(out var item) && !_cancellationTokenSource.IsCancellationRequested)
                    {
                        needRefresh = false;

                        if (!item.Action(ref item, sequence))
                            hasPending = true;

                        sequence++;
                        if ((sequence & 0x7F) == 0)
                        {
                            if (hasPending)
                            {
                                _faster.CompletePending(true);
                                hasPending = false;
                            }

                            _faster.Refresh();
                        }
                    }

                    if (hasPending)
                    {
                        _faster.CompletePending(true);
                        hasPending = false;
                    }

                    if (needRefresh)
                    {
                        _faster.Refresh();
                    }
                }
            }
            catch (Exception ex)
            {
                Trace.Write(ex);
            }

            var isRunning = true;
            while (isRunning)
            {
                var lockTaken = false;
                try
                {
                    _spinLock.Enter(ref lockTaken);

                    isRunning = Volatile.Read(ref _checkpointRunning) > 0;
                }
                finally
                {
                    if (lockTaken)
                        _spinLock.Exit();
                }

                _faster.Refresh();
                _faster.CompletePending(true);
                Thread.Sleep(10);
            }

            _faster.StopSession();
        }

        private void CheckpointLoop()
        {
            try
            {
                var checkpointIntervalMilliseconds = _settings.CheckpointIntervalMilliseconds;
                var takenCheckpoints = new Guid[_settings.CheckpointCleanCount];
                var takenCount = 0;
                var checkpointTokenFile = new FileInfo(Path.Combine(_directory.FullName, "checkpoint_token.txt"));
                var checkpointTokenBackup = new FileInfo(Path.Combine(_directory.FullName, "checkpoint_token_backup.txt"));
                var currentCheckpointVersion = Volatile.Read(ref _checkpointVersion);

                while (!_cancellationTokenSource.IsCancellationRequested)
                {
                    var checkpointItem = default(MisterCheckpointItem);
                    lock (_checkpointQueue)
                    {
                        if (_checkpointQueue.IsEmpty && !_cancellationTokenSource.IsCancellationRequested)
                            Monitor.Wait(_checkpointQueue, checkpointIntervalMilliseconds);
                    }

                    _checkpointQueue.TryDequeue(out checkpointItem);

                    var lockTaken = false;
                    try
                    {
                        _spinLock.Enter(ref lockTaken);
                        if (_cancellationTokenSource.IsCancellationRequested)
                            break;

                        Volatile.Write(ref _checkpointRunning, 1);
                    }
                    finally
                    {
                        if (lockTaken)
                            _spinLock.Exit();
                    }

                    var newCheckpoint = Volatile.Read(ref _checkpointVersion);
                    if (newCheckpoint != currentCheckpointVersion)
                    {
                        currentCheckpointVersion = newCheckpoint;

                        _faster.StartSession();
                        _faster.TakeFullCheckpoint(out var token);
                        _faster.CompleteCheckpoint(true);
                        _faster.StopSession();

                        takenCheckpoints[takenCount++] = token;
                        if (takenCount >= takenCheckpoints.Length)
                        {
                            for (var i = 0; i < takenCheckpoints.Length - 2; i++)
                            {
                                try
                                {
                                    var info = new DirectoryInfo(Path.Combine(_directory.FullName, "cpr-checkpoints", takenCheckpoints[i].ToString()));
                                    if (info.Exists)
                                        info.Delete(true);
                                }
                                catch { }

                                try
                                {
                                    var info = new DirectoryInfo(Path.Combine(_directory.FullName, "index-checkpoints", takenCheckpoints[i].ToString()));
                                    if (info.Exists)
                                        info.Delete(true);
                                }
                                catch { }
                            }

                            takenCheckpoints[0] = takenCheckpoints[takenCheckpoints.Length - 2];
                            takenCheckpoints[1] = takenCheckpoints[takenCheckpoints.Length - 1];
                            takenCount = 2;
                        }

                        try
                        {
                            if (!checkpointTokenFile.Exists)
                            {
                                using (var writer = checkpointTokenFile.CreateText())
                                    writer.WriteLine(token.ToString());

                                checkpointTokenFile.Refresh();
                            }
                            else
                            {
                                var temp = new FileInfo(Path.Combine(_directory.FullName, $"checkpoint_{token}.txt"));
                                using (var writer = temp.CreateText())
                                    writer.WriteLine(token.ToString());

                                File.Replace(temp.FullName, checkpointTokenFile.FullName, checkpointTokenBackup.FullName, true);
                            }
                        }
                        catch (Exception ex)
                        {
                            Log.Error(ex);
                        }
                    }

                    lockTaken = false;
                    try
                    {
                        _spinLock.Enter(ref lockTaken);

                        Volatile.Write(ref _checkpointRunning, 0);
                    }
                    finally
                    {
                        if (lockTaken)
                            _spinLock.Exit();
                    }

                    if (checkpointItem.ResetEvent != null)
                        checkpointItem.ResetEvent.Set();

                    if (checkpointItem.TaskCompletionSource != null)
                        checkpointItem.TaskCompletionSource.SetResult(MisterVoid.Value);
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex);
            }

            Volatile.Write(ref _checkpointRunning, 0);
        }
    }
}
