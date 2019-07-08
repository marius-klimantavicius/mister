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

        private struct MisterWorkItem
        {
            public TKey Key;
            public TValue Value;
            public object State;
            public Action<TKey, TValue, object, long> Action;
        }

        private readonly DirectoryInfo _directory;
        private readonly IMisterSerializer<TKey> _keySerializer;
        private readonly IMisterSerializer<TValue> _valueSerializer;
        private readonly MisterConnectionSettings _settings;

        private readonly RecyclableMemoryStreamManager _streamManager;
        private readonly CancellationTokenSource _cancellationTokenSource;

        private FasterKV<MisterObject, MisterObject, byte[], TValue, Empty, MisterObjectEnvironment<TValue>> _faster;
        private IDevice _mainLog;

        private BlockingCollection<MisterWorkItem> _workQueue;
        private Thread[] _workerThreads;

        private BlockingCollection<AutoResetEvent> _checkpointQueue;
        private Thread _checkpointThread;

        private int _checkpointVersion;
        private int _checkpointRunning;
        private SpinLock _spinLock = new SpinLock();

        private bool _isClosed;

        public MisterConnection(DirectoryInfo directory, IMisterSerializer<TKey> keySerializer, IMisterSerializer<TValue> valueSerializer, MisterConnectionSettings settings = null)
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

            _streamManager = new RecyclableMemoryStreamManager(1024, 4 * 1024, 1024 * 1024);
            _cancellationTokenSource = new CancellationTokenSource();

            Initialize();
        }

        public void Close()
        {
            if (_isClosed)
                return;

            Checkpoint();

            _isClosed = true;
            _cancellationTokenSource.Cancel();

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

            var handle = new AutoResetEvent(false);
            _checkpointQueue.Add(handle);
            handle.WaitOne();
        }

        public Task<TValue> Get(TKey key)
        {
            if (_isClosed)
                throw new ObjectDisposedException("MisterConnection");

            var tsc = new TaskCompletionSource<TValue>(TaskCreationOptions.RunContinuationsAsynchronously);
            _workQueue.Add(new MisterWorkItem()
            {
                Key = key,
                State = tsc,
                Action = PerformGet,
            });
            return tsc.Task;
        }

        public Task Set(TKey key, TValue value)
        {
            if (_isClosed)
                throw new ObjectDisposedException("MisterConnection");

            var tsc = new TaskCompletionSource<MisterVoid>(TaskCreationOptions.RunContinuationsAsynchronously);
            _workQueue.Add(new MisterWorkItem()
            {
                Key = key,
                Value = value,
                State = tsc,
                Action = PerformSet,
            });
            return tsc.Task;
        }

        public Task Delete(TKey key)
        {
            if (_isClosed)
                throw new ObjectDisposedException("MisterConnection");

            var tsc = new TaskCompletionSource<MisterVoid>(TaskCreationOptions.RunContinuationsAsynchronously);
            _workQueue.Add(new MisterWorkItem()
            {
                Key = key,
                State = tsc,
                Action = PerformDelete,
            });
            return tsc.Task;
        }

        private unsafe void PerformGet(TKey key, TValue value, object state, long sequence)
        {
            try
            {
                var input = default(byte[]);
                var output = default(TValue);
                var status = default(Status);
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

                if (state == null)
                    return;

                var tsc = Unsafe.As<TaskCompletionSource<TValue>>(state);
                if (status == Status.ERROR)
                    tsc.SetException(new Exception());
                else
                    tsc.SetResult(output);
            }
            catch (Exception ex)
            {
                var tsc = (TaskCompletionSource<TValue>)state;
                tsc.SetException(ex);
            }
        }

        private unsafe void PerformSet(TKey key, TValue value, object state, long sequence)
        {
            try
            {
                var status = default(Status);
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
                            if (status == Status.PENDING)
                                _faster.CompletePending(true);
                        } while (status == Status.PENDING);

                        Interlocked.Increment(ref _checkpointVersion);
                    }
                }


                if (state == null)
                    return;

                var tsc = Unsafe.As<TaskCompletionSource<MisterVoid>>(state);
                if (status == Status.ERROR)
                    tsc.SetException(new Exception());
                else
                    tsc.SetResult(default(MisterVoid));
            }
            catch (Exception ex)
            {
                var tsc = (TaskCompletionSource<MisterVoid>)state;
                tsc.SetException(ex);
            }
        }

        private unsafe void PerformDelete(TKey key, TValue value, object state, long sequence)
        {
            try
            {
                var status = default(Status);
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
                            if (status == Status.PENDING)
                                _faster.CompletePending(true);
                        } while (status == Status.PENDING);

                        Interlocked.Increment(ref _checkpointVersion);
                    }
                }

                if (state == null)
                    return;

                var tsc = Unsafe.As<TaskCompletionSource<MisterVoid>>(state);
                if (status == Status.ERROR)
                    tsc.SetException(new Exception());
                else
                    tsc.SetResult(default(MisterVoid));
            }
            catch (Exception ex)
            {
                if (state == null)
                    return;

                var tsc = (TaskCompletionSource<MisterVoid>)state;
                tsc.SetException(ex);
            }
        }

        private void Initialize()
        {
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
                new CheckpointSettings() { CheckpointDir = _directory.FullName, CheckPointType = CheckpointType.Snapshot },
                serializerSettings: null,
                comparer: new MisterObjectEqualityComparer(),
                variableLengthStructSettings: variableLengthStructSettings
            );

            var checkpointTokenFile = new FileInfo(Path.Combine(_directory.FullName, "checkpoint_token.txt"));
            var checkpointTokenBackupFile = new FileInfo(Path.Combine(_directory.FullName, "checkpoint_token_backup.txt"));

            CleanOldCheckpoints(checkpointTokenFile, checkpointTokenBackupFile);

            if (!TryRecover(checkpointTokenFile))
                TryRecover(checkpointTokenBackupFile);

            _workQueue = new BlockingCollection<MisterWorkItem>();
            _workerThreads = new Thread[_settings.WorkerThreadCount];
            for (var i = 0; i < _workerThreads.Length; i++)
                _workerThreads[i] = new Thread(WorkerLoop) { IsBackground = true };

            _checkpointQueue = new BlockingCollection<AutoResetEvent>();
            _checkpointThread = new Thread(CheckpointLoop) { IsBackground = true };

            for (var i = 0; i < _workerThreads.Length; i++)
                _workerThreads[i].Start(i.ToString());

            _checkpointThread.Start();
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
                        _faster.Recover(checkpointToken.Value);
                    else
                        checkpointTokenFile.Delete();

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

                while (!_cancellationTokenSource.IsCancellationRequested)
                {
                    if (_workQueue.TryTake(out var item, workerRefreshIntervalMilliseconds))
                    {
                        item.Action(item.Key, item.Value, item.State, sequence);
                        sequence++;

                        if ((sequence & 0x7F) == 0)
                            _faster.Refresh();
                    }
                    else
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
                    var waitHandle = default(AutoResetEvent);
                    try
                    {
                        _checkpointQueue.TryTake(out waitHandle, checkpointIntervalMilliseconds, _cancellationTokenSource.Token);
                    }
                    catch (OperationCanceledException) { }

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

                        Trace.WriteLine("Checkpoint...");

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

                        Trace.WriteLine("...complete");
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

                    if (waitHandle != null)
                        waitHandle.Set();
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
