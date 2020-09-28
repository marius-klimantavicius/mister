﻿using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;

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
        public IMisterSession<TKey, TValue> CreateSession(string sessionId = null)
        {
            return _underlyingConnection.CreateSession(sessionId);
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
        public void Flush(bool waitPending, bool evict = false)
        {
            _underlyingConnection.Flush(waitPending);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TValue> GetAsync(TKey key)
        {
            return _underlyingConnection.GetAsync(key);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TValue> GetAsync(TKey key, bool waitCommit)
        {
            return _underlyingConnection.GetAsync(key, waitCommit);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask SetAsync(TKey key, TValue value)
        {
            return _underlyingConnection.SetAsync(key, value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask SetAsync(TKey key, TValue value, bool waitCommit)
        {
            return _underlyingConnection.SetAsync(key, value, waitCommit);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DeleteAsync(TKey key)
        {
            return _underlyingConnection.DeleteAsync(key);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DeleteAsync(TKey key, bool waitCommit)
        {
            return _underlyingConnection.DeleteAsync(key, waitCommit);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ForEach<TState>(Action<TKey, TValue, bool, TState> onRecord, Action<TState> onCompleted = null, TState state = default(TState))
        {
            _underlyingConnection.ForEach(onRecord, onCompleted, state);
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
            FasterKV<MisterObject, MisterObject>
        >
        where TKeyAtomSource : struct, IMisterAtomSource<MisterObject>
        where TValueAtomSource : struct, IMisterAtomSource<MisterObject>
    {
        private sealed class MisterVariableLengthStruct : IVariableLengthStruct<MisterObject, byte[]>
        {
            public static readonly MisterVariableLengthStruct Instance = new MisterVariableLengthStruct();

            public int GetInitialLength(ref byte[] input)
            {
                return sizeof(int) + input.Length;
            }

            public int GetLength(ref MisterObject t, ref byte[] input)
            {
                return sizeof(int) + input.Length;
            }
        }

        private readonly MisterObjectEnvironment<TValue, TValueAtomSource> _environment;

        public MisterConnection(DirectoryInfo directory, IMisterSerializer<TKey, MisterObject, TKeyAtomSource> keySerializer, IMisterSerializer<TValue, MisterObject, TValueAtomSource> valueSerializer, MisterConnectionSettings settings = null, string name = null)
            : base(directory, keySerializer, valueSerializer, settings, name)
        {
            _environment = new MisterObjectEnvironment<TValue, TValueAtomSource>(_valueSerializer);

            Initialize();
        }

        protected override void Create()
        {
            if (_faster != null)
                _faster.Dispose();

            if (_mainDevice != null)
                _mainDevice.Dispose();

            var variableLengthStructSettings = new VariableLengthStructSettings<MisterObject, MisterObject>()
            {
                keyLength = MisterObjectVariableLengthStruct.Instance,
                valueLength = MisterObjectVariableLengthStruct.Instance,
            };

            _mainDevice = Devices.CreateLogDevice(Path.Combine(_directory.FullName, @"hlog.log"));
            _faster = new FasterKV<MisterObject, MisterObject>(
                _settings.IndexSize,
                _settings.GetLogSettings(_mainDevice),
                new CheckpointSettings() { CheckpointDir = _directory.FullName, CheckPointType = CheckpointType.FoldOver },
                comparer: MisterObjectEqualityComparer.Instance,
                variableLengthStructSettings: variableLengthStructSettings
            );
        }

        protected override ClientSession<MisterObject, MisterObject, byte[], TValue, object, MisterObjectEnvironment<TValue, TValueAtomSource>> NewSession(string sessionId)
        {
            return _faster.For(_environment).NewSession(_environment, sessionId, variableLengthStruct: MisterVariableLengthStruct.Instance);
        }
    }

    public abstract class MisterConnection<TKey, TValue, TKeyAtom, TKeyAtomSource, TValueAtom, TValueAtomSource, TFunctions, TFaster> : IMisterConnection<TKey, TValue>
        where TKeyAtomSource : struct, IMisterAtomSource<TKeyAtom>
        where TValueAtomSource : struct, IMisterAtomSource<TValueAtom>
        where TFunctions : IFunctions<TKeyAtom, TValueAtom, byte[], TValue, object>
        where TFaster : IFasterKV<TKeyAtom, TValueAtom>
    {
        protected sealed class MisterSession : IMisterSession<TKey, TValue>
        {
            private readonly MisterConnection<TKey, TValue, TKeyAtom, TKeyAtomSource, TValueAtom, TValueAtomSource, TFunctions, TFaster> _connection;
            private readonly ClientSession<TKeyAtom, TValueAtom, byte[], TValue, object, TFunctions> _session;
            private readonly CancellationToken _cancellationToken;
            private bool _isDisposed;

            public ClientSession<TKeyAtom, TValueAtom, byte[], TValue, object, TFunctions> Session => _session;

            public MisterSession Prev;
            public MisterSession Next;

            public MisterSession(MisterConnection<TKey, TValue, TKeyAtom, TKeyAtomSource, TValueAtom, TValueAtomSource, TFunctions, TFaster> connection, string sessionId = null)
            {
                _connection = connection;
                _session = _connection.NewSession(sessionId);

                _cancellationToken = _connection._cancellationTokenSource.Token;

                Interlocked.Increment(ref _connection._sessionsStarted);

                Prev = this;
                Next = this;
            }

            public void Dispose()
            {
                if (!_isDisposed)
                {
                    _isDisposed = true;

                    try
                    {
                        _session.Dispose();
                    }
                    catch { }

                    _connection.RemoveSession(this);
                }
            }

            public async ValueTask<TValue> GetAsync(TKey key, bool waitCommit = false)
            {
                if (_isDisposed || _cancellationToken.IsCancellationRequested)
                    throw new ObjectDisposedException("Session");

                using (var source = _connection._keySerializer.Serialize(key))
                {
                    var input = default(byte[]);
                    var readResult = await _session.ReadAsync(ref source.GetAtom(), ref input, token: _cancellationToken);
                    if (waitCommit)
                        await _session.WaitForCommitAsync();

                    var result = readResult.Complete();
                    return result.Item2;
                }
            }

            public async ValueTask SetAsync(TKey key, TValue value, bool waitCommit = false)
            {
                if (_isDisposed || _cancellationToken.IsCancellationRequested)
                    throw new ObjectDisposedException("MisterSession");

                using (var keySource = _connection._keySerializer.Serialize(key))
                using (var valueSource = _connection._valueSerializer.Serialize(value))
                {
                    var status = _session.Upsert(ref keySource.GetAtom(), ref valueSource.GetAtom());
                    if (status == Status.PENDING)
                        await _session.CompletePendingAsync(waitForCommit: waitCommit, token: _cancellationToken);
                    _connection._maintenanceService.IncrementVersion();
                }
            }

            public async ValueTask DeleteAsync(TKey key, bool waitCommit = false)
            {
                if (_isDisposed || _cancellationToken.IsCancellationRequested)
                    throw new ObjectDisposedException("Session");

                using (var keySource = _connection._keySerializer.Serialize(key))
                {
                    var status = _session.Delete(ref keySource.GetAtom());
                    if (status == Status.PENDING)
                        await _session.CompletePendingAsync(waitForCommit: waitCommit, token: _cancellationToken);
                    _connection._maintenanceService.IncrementVersion();
                }
            }
        }

        protected readonly DirectoryInfo _directory;
        protected readonly IMisterSerializer<TKey, TKeyAtom, TKeyAtomSource> _keySerializer;
        protected readonly IMisterSerializer<TValue, TValueAtom, TValueAtomSource> _valueSerializer;
        protected readonly MisterConnectionSettings _settings;
        protected readonly string _name;
        protected readonly MisterConnectionMaintenanceService<TValue, TKeyAtom, TValueAtom, TFunctions, TFaster> _maintenanceService;

        private readonly CancellationTokenSource _cancellationTokenSource;
        private bool _isClosed;

        private readonly object _lock = new object();

        private readonly int _sessionPoolSize = Environment.ProcessorCount * 2;
        private readonly ConcurrentBag<MisterSession> _sessionPool = new ConcurrentBag<MisterSession>();

        private MisterSession _sessionRoot;
        private int _sessionsStarted;

        protected TFaster _faster;
        protected IDevice _mainDevice;

        public string Name => _name;

        protected MisterConnection(DirectoryInfo directory, IMisterSerializer<TKey, TKeyAtom, TKeyAtomSource> keySerializer, IMisterSerializer<TValue, TValueAtom, TValueAtomSource> valueSerializer, MisterConnectionSettings settings = null, string name = null)
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
        }

        public void Close()
        {
            if (_isClosed)
                return;

            lock (_lock)
            {
                _isClosed = true;

                _cancellationTokenSource.Cancel();

                _maintenanceService.Stop();

                while (_sessionRoot != null)
                    _sessionRoot.Dispose();

                _maintenanceService.Close();

                _faster.Dispose();
                _mainDevice.Dispose();
                _cancellationTokenSource.Dispose();
            }
        }

        public IMisterSession<TKey, TValue> CreateSession(string sessionId = null)
        {
            return GetOrCreateSession(sessionId);
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

        public void Flush(bool waitPending, bool evict = false)
        {
            if (evict)
                _faster.Log.FlushAndEvict(waitPending);
            else
                _faster.Log.Flush(waitPending);

            _maintenanceService.IncrementVersion();
        }

        public async ValueTask<TValue> GetAsync(TKey key, bool waitCommit = false)
        {
            var session = GetOrCreateSession();
            try
            {
                return await session.GetAsync(key, waitCommit);
            }
            finally
            {
                ReturnOrDisposeSession(session);
            }
        }

        public async ValueTask SetAsync(TKey key, TValue value, bool waitCommit = false)
        {
            var session = GetOrCreateSession();
            try
            {
                await session.SetAsync(key, value, waitCommit);
            }
            finally
            {
                ReturnOrDisposeSession(session);
            }
        }

        public async ValueTask DeleteAsync(TKey key, bool waitCommit = false)
        {
            var session = GetOrCreateSession();
            try
            {
                await session.GetAsync(key, waitCommit);
            }
            finally
            {
                ReturnOrDisposeSession(session);
            }
        }

        public void ForEach<TState>(Action<TKey, TValue, bool, TState> onRecord, Action<TState> onCompleted = null, TState state = default(TState))
        {
            CheckDisposed();

            if (onRecord == null)
                throw new ArgumentNullException(nameof(onRecord));

            var iterator = _faster.Log.Scan(_faster.Log.BeginAddress, _faster.Log.TailAddress);
            while (iterator.GetNext(out var recordInfo))
            {
                ref var misterKey = ref iterator.GetKey();
                ref var misterValue = ref iterator.GetValue();

                var key = _keySerializer.Deserialize(ref misterKey);
                var value = _valueSerializer.Deserialize(ref misterValue);

                var isDeleted = recordInfo.Tombstone;
                onRecord(key, value, isDeleted, state);
            }

            if (onCompleted != null)
                onCompleted(state);
        }

        protected abstract void Create();

        protected abstract ClientSession<TKeyAtom, TValueAtom, byte[], TValue, object, TFunctions> NewSession(string sessionId);

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
                _maintenanceService.Start();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckDisposed()
        {
            if (_isClosed)
                throw new ObjectDisposedException("MisterConnection");
        }

        protected MisterSession GetOrCreateSession(string sessionId = null)
        {
            CheckDisposed();

            if (string.IsNullOrEmpty(sessionId) && _sessionPool.TryTake(out var existing))
                return existing;

            lock (_lock)
            {
                var newSession = new MisterSession(this, sessionId);
                if (_sessionRoot == null)
                {
                    _sessionRoot = newSession;
                }
                else
                {
                    newSession.Next = _sessionRoot;
                    newSession.Prev = _sessionRoot.Prev;
                    _sessionRoot.Prev.Next = newSession;
                    _sessionRoot.Prev = newSession;
                }

                Interlocked.Increment(ref _sessionsStarted);
                return newSession;
            }
        }

        protected void ReturnOrDisposeSession(MisterSession session)
        {
            if (_sessionPool.Count < _sessionPoolSize)
                _sessionPool.Add(session);
            else
                session.Dispose();
        }

        private void RemoveSession(MisterSession session)
        {
            Debug.Assert(_sessionRoot != null);

            lock (_lock)
            {
                if (session.Next == session)
                {
                    Debug.Assert(session == _sessionRoot);
                    _sessionRoot = null;
                }
                else
                {
                    session.Next.Prev = session.Prev;
                    session.Prev.Next = session.Next;

                    if (_sessionRoot == session)
                        _sessionRoot = session.Next;
                }

                Interlocked.Decrement(ref _sessionsStarted);
            }
        }
    }
}
