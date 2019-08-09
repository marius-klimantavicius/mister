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
using Microsoft.IO;

namespace Marius.Mister
{
    public sealed class MisterConnection<TKey, TValue>
    {
        private static readonly ILog Log = LogManager.GetLogger("MisterConnection");

        private struct MisterVoid
        {
        }

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

        private readonly DirectoryInfo _directory;
        private readonly IMisterSerializer<TKey> _keySerializer;
        private readonly IMisterSerializer<TValue> _valueSerializer;
        private readonly MisterConnectionSettings _settings;
        private readonly string _name;
        private readonly RecyclableMemoryStreamManager _streamManager;
        private readonly CancellationTokenSource _cancellationTokenSource;

        private FasterKV<MisterObject, MisterObject, byte[], TValue, Empty, MisterObjectEnvironment<TValue>> _faster;
        private IDevice _mainLog;

        private ConcurrentQueue<MisterWorkItem> _workQueue;
        private Thread[] _workerThreads;

        private ConcurrentQueue<MisterCheckpointItem> _checkpointQueue;
        private Thread _checkpointThread;

        private int _checkpointVersion;
        private int _checkpointRunning;
        private SpinLock _spinLock = new SpinLock();

        private bool _isClosed;

        public MisterConnection(DirectoryInfo directory, IMisterSerializer<TKey> keySerializer, IMisterSerializer<TValue> valueSerializer, MisterConnectionSettings settings = null, string name = null)
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
            _streamManager = new RecyclableMemoryStreamManager(1024, 4 * 1024, 1024 * 1024);
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
            if (_isClosed)
                throw new ObjectDisposedException("MisterConnection");

            PerformCheckpoint();
        }

        public Task CheckpointAsync()
        {
            if (_isClosed)
                throw new ObjectDisposedException("MisterConnection");

            return PerformCheckpointAsync();
        }

        public Task<TValue> GetAsync(TKey key)
        {
            if (_isClosed)
                throw new ObjectDisposedException("MisterConnection");

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

        public Task SetAsync(TKey key, TValue value)
        {
            if (_isClosed)
                throw new ObjectDisposedException("MisterConnection");

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
            if (_isClosed)
                throw new ObjectDisposedException("MisterConnection");

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
            if (_isClosed)
                throw new ObjectDisposedException("MisterConnection");

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
            if (_isClosed)
                throw new ObjectDisposedException("MisterConnection");

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

        private unsafe bool PerformGet(ref MisterWorkItem workItem, long sequence)
        {
            var status = default(Status);
            var key = workItem.Key;
            var state = workItem.State;

            try
            {
                var input = default(byte[]);
                var output = default(TValue);
                using (var keyStream = _streamManager.GetStream())
                {
                    keyStream.Position = 4;
                    _keySerializer.Serialize(keyStream, key);

                    var keyLength = (int)keyStream.Length - 4;
                    var keyBuffer = keyStream.GetBuffer();

                    fixed (byte* keyPointer = keyBuffer)
                    {
                        ref var misterKey = ref *(MisterObject*)keyPointer;
                        misterKey.Length = keyLength;

                        do
                        {
                            status = _faster.Read(ref misterKey, ref input, ref output, Empty.Default, sequence);
                            if (status == Status.PENDING)
                                _faster.CompletePending(true);
                        }
                        while (status == Status.PENDING);
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
                using (var keyStream = _streamManager.GetStream())
                using (var valueStream = _streamManager.GetStream())
                {
                    keyStream.Position = 4;
                    _keySerializer.Serialize(keyStream, key);

                    valueStream.Position = 4;
                    _valueSerializer.Serialize(valueStream, value);

                    var keyLength = (int)keyStream.Length - 4;
                    var keyBuffer = keyStream.GetBuffer();

                    var valueLength = (int)valueStream.Length - 4;
                    var valueBuffer = valueStream.GetBuffer();

                    fixed (byte* keyPointer = keyBuffer, valuePointer = valueBuffer)
                    {
                        ref var misterKey = ref *(MisterObject*)keyPointer;
                        misterKey.Length = keyLength;

                        ref var misterValue = ref *(MisterObject*)valuePointer;
                        misterValue.Length = valueLength;

                        do
                        {
                            status = _faster.Upsert(ref misterKey, ref misterValue, Empty.Default, sequence);
                            if (workItem.WaitPending && status == Status.PENDING)
                                _faster.CompletePending(true);
                        } while (workItem.WaitPending && status == Status.PENDING);

                        Interlocked.Increment(ref _checkpointVersion);
                    }
                }

                if (state != null)
                {
                    var tsc = Unsafe.As<TaskCompletionSource<MisterVoid>>(state);
                    if (status == Status.ERROR)
                        tsc.SetException(new Exception());
                    else
                        tsc.SetResult(default(MisterVoid));
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
                using (var keyStream = _streamManager.GetStream())
                {
                    keyStream.Position = 4;
                    _keySerializer.Serialize(keyStream, key);

                    var length = (int)keyStream.Length - 4;
                    var buffer = keyStream.GetBuffer();
                    fixed (byte* keyPointer = buffer)
                    {
                        ref var misterKey = ref *(MisterObject*)keyPointer;
                        misterKey.Length = length;

                        do
                        {
                            status = _faster.Delete(ref misterKey, Empty.Default, sequence);
                            if (workItem.WaitPending && status == Status.PENDING)
                                _faster.CompletePending(true);
                        } while (workItem.WaitPending && status == Status.PENDING);

                        Interlocked.Increment(ref _checkpointVersion);
                    }
                }

                if (state != null)
                {
                    var tsc = Unsafe.As<TaskCompletionSource<MisterVoid>>(state);
                    if (status == Status.ERROR)
                        tsc.SetException(new Exception());
                    else
                        tsc.SetResult(default(MisterVoid));
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

        private void PerformCheckpoint()
        {
            var handle = new AutoResetEvent(false);
            _checkpointQueue.Enqueue(new MisterCheckpointItem(handle));

            lock (_checkpointQueue)
                Monitor.Pulse(_checkpointQueue);

            handle.WaitOne();
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

            var variableLengthStructSettings = new VariableLengthStructSettings<MisterObject, MisterObject>()
            {
                keyLength = new MisterObjectVariableLengthStruct(),
                valueLength = new MisterObjectVariableLengthStruct(),
            };

            _mainLog = Devices.CreateLogDevice(Path.Combine(_directory.FullName, @"hlog.log"));
            _faster = new FasterKV<MisterObject, MisterObject, byte[], TValue, Empty, MisterObjectEnvironment<TValue>>(
                1L << 20,
                new MisterObjectEnvironment<TValue>(_valueSerializer),
                new LogSettings { LogDevice = _mainLog, },
                new CheckpointSettings() { CheckpointDir = _directory.FullName, CheckPointType = CheckpointType.FoldOver },
                serializerSettings: null,
                comparer: new MisterObjectEqualityComparer(),
                variableLengthStructSettings: variableLengthStructSettings
            );
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

                    if (needRefresh)
                    {
                        if (hasPending)
                        {
                            _faster.CompletePending(true);
                            hasPending = false;
                        }

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
                        checkpointItem.TaskCompletionSource.SetResult(default(MisterVoid));
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
