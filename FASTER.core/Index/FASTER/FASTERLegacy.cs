// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    public sealed class FasterKV<Key, Value, Input, Output, Context, Functions> : IFasterKV<Key, Value, Input, Output, Context, Functions>, IDisposable
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        private readonly FasterKV<Key, Value, Input, Output, Context> _inner;
        private readonly Functions _functions;

        private FastThreadLocal<FasterKV<Key, Value, Input, Output, Context>.FasterExecutionContext<Functions>> threadCtx;

        public long EntryCount => _inner.EntryCount;

        public long IndexSize => _inner.IndexSize;

        public IFasterEqualityComparer<Key> Comparer => _inner.Comparer;

        public LogAccessor<Key, Value, Input, Output, Context> Log => _inner.Log;

        public LogAccessor<Key, Value, Input, Output, Context> ReadCache => _inner.ReadCache;

        public FasterKV(long size, Functions functions, LogSettings logSettings,
            CheckpointSettings checkpointSettings = null, SerializerSettings<Key, Value> serializerSettings = null,
            IFasterEqualityComparer<Key> comparer = null,
            VariableLengthStructSettings<Key, Value> variableLengthStructSettings = null)
        {
            _inner = new FasterKV<Key, Value, Input, Output, Context>(size, logSettings, checkpointSettings, serializerSettings, comparer, variableLengthStructSettings);
            _functions = functions;
        }

        public void Dispose()
        {
            LegacyDispose();
            _inner.Dispose();
        }

        public ValueTask CompleteCheckpointAsync(CancellationToken token = default(CancellationToken)) => _inner.CompleteCheckpointAsync(token);

        public string DumpDistribution() => _inner.DumpDistribution();

        public bool GrowIndex() => _inner.GrowIndex();

        public ClientSession<Key, Value, Input, Output, Context, Functions> NewSession(string sessionId = null, bool threadAffinitized = false)
            => _inner.NewSession(_functions, sessionId, threadAffinitized);

        public void Recover() => _inner.Recover();

        public void Recover(Guid fullcheckpointToken) => _inner.Recover(fullcheckpointToken);

        public void Recover(Guid indexToken, Guid hybridLogToken) => _inner.Recover(indexToken, hybridLogToken);

        public ClientSession<Key, Value, Input, Output, Context, Functions> ResumeSession(string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false)
            => _inner.ResumeSession(sessionId, _functions, out commitPoint, threadAffinitized);

        public bool TakeFullCheckpoint(out Guid token, long targetVersion = -1) => _inner.TakeFullCheckpoint(out token, targetVersion);

        public bool TakeHybridLogCheckpoint(out Guid token, long targetVersion = -1) => _inner.TakeHybridLogCheckpoint(out token, targetVersion);

        public bool TakeIndexCheckpoint(out Guid token) => _inner.TakeIndexCheckpoint(out token);

        /// <summary>
        /// Legacy API: Start session with FASTER - call once per thread before using FASTER
        /// </summary>
        /// <returns></returns>
        [Obsolete("Use NewSession() instead.")]
        public Guid StartSession()
        {
            if (threadCtx == null)
                threadCtx = new FastThreadLocal<FasterKV<Key, Value, Input, Output, Context>.FasterExecutionContext<Functions>>();

            return InternalAcquire();
        }

        /// <summary>
        /// Legacy API: Continue session with FASTER
        /// </summary>
        /// <param name="guid"></param>
        /// <returns></returns>
        [Obsolete("Use ResumeSession() instead.")]
        public CommitPoint ContinueSession(Guid guid)
        {
            StartSession();

            var cp = _inner.InternalContinue<Functions>(guid.ToString(), _functions, out var ctx);
            threadCtx.Value = ctx;

            return cp;
        }

        /// <summary>
        ///  Legacy API: Stop session with FASTER
        /// </summary>
        [Obsolete("Use and dispose NewSession() instead.")]
        public void StopSession()
        {
            InternalRelease(this.threadCtx.Value);
        }

        /// <summary>
        ///  Legacy API: Refresh epoch (release memory pins)
        /// </summary>
        [Obsolete("Use NewSession(), where Refresh() is not required by default.")]
        public void Refresh()
        {
            _inner.InternalRefresh(threadCtx.Value);
        }

        /// <summary>
        ///  Legacy API: Complete all pending operations issued by this session
        /// </summary>
        /// <param name="wait">Whether we spin-wait for pending operations to complete</param>
        /// <returns>Whether all pending operations have completed</returns>
        [Obsolete("Use NewSession() and invoke CompletePending() on the session.")]
        public bool CompletePending(bool wait = false)
        {
            return _inner.InternalCompletePending(threadCtx.Value, wait);
        }

        /// <summary>
        /// Legacy API: Read operation
        /// </summary>
        /// <param name="key">Key of read</param>
        /// <param name="input">Input argument used by Reader to select what part of value to read</param>
        /// <param name="output">Reader stores the read result in output</param>
        /// <param name="context">User context to identify operation in asynchronous callback</param>
        /// <param name="serialNo">Increasing sequence number of operation (used for recovery)</param>
        /// <returns>Status of operation</returns>
        [Obsolete("Use NewSession() and invoke Read() on the session.")]
        public Status Read(ref Key key, ref Input input, ref Output output, Context context, long serialNo)
        {
            return _inner.ContextRead(ref key, ref input, ref output, context, serialNo, threadCtx.Value);
        }

        /// <summary>
        /// Legacy API: (Blind) upsert operation
        /// </summary>
        /// <param name="key">Key of read</param>
        /// <param name="value">Value being upserted</param>
        /// <param name="context">User context to identify operation in asynchronous callback</param>
        /// <param name="serialNo">Increasing sequence number of operation (used for recovery)</param>
        /// <returns>Status of operation</returns>
        [Obsolete("Use NewSession() and invoke Upsert() on the session.")]
        public Status Upsert(ref Key key, ref Value value, Context context, long serialNo)
        {
            return _inner.ContextUpsert(ref key, ref value, context, serialNo, threadCtx.Value);
        }

        /// <summary>
        /// Atomic read-modify-write operation
        /// </summary>
        /// <param name="key">Key of read</param>
        /// <param name="input">Input argument used by RMW callback to perform operation</param>
        /// <param name="context">User context to identify operation in asynchronous callback</param>
        /// <param name="serialNo">Increasing sequence number of operation (used for recovery)</param>
        /// <returns>Status of operation</returns>
        [Obsolete("Use NewSession() and invoke RMW() on the session.")]
        public Status RMW(ref Key key, ref Input input, Context context, long serialNo)
        {
            return _inner.ContextRMW(ref key, ref input, context, serialNo, threadCtx.Value);
        }

        /// <summary>
        /// Delete entry (use tombstone if necessary)
        /// Hash entry is removed as a best effort (if key is in memory and at 
        /// the head of hash chain.
        /// Value is set to null (using ConcurrentWrite) if it is in mutable region
        /// </summary>
        /// <param name="key">Key of delete</param>
        /// <param name="context">User context to identify operation in asynchronous callback</param>
        /// <param name="serialNo">Increasing sequence number of operation (used for recovery)</param>
        /// <returns>Status of operation</returns>
        [Obsolete("Use NewSession() and invoke Delete() on the session.")]
        public Status Delete(ref Key key, Context context, long serialNo)
        {
            return _inner.ContextDelete(ref key, context, serialNo, threadCtx.Value);
        }

        /// <summary>
        /// Legacy API: Complete the ongoing checkpoint (if any)
        /// </summary>
        /// <param name="spinWait">Spin-wait for completion</param>
        /// <returns></returns>
        [Obsolete("Use NewSession() and CompleteCheckpointAsync() instead.")]
        public bool CompleteCheckpoint(bool spinWait = false)
        {
            if (!InLegacySession())
            {
                _inner.CompleteCheckpointAsync().GetAwaiter().GetResult();
                return true;
            }

            // the thread has an active legacy session
            // so we need to constantly complete pending 
            // and refresh (done inside CompletePending)
            // for the checkpoint to be proceed
            do
            {
                CompletePending();
                if (_inner._systemState.phase == Phase.REST)
                {
                    CompletePending();
                    return true;
                }
            } while (spinWait);

            return false;
        }

        /// <summary>
        /// Dispose FASTER instance - legacy items
        /// </summary>
        private void LegacyDispose()
        {
            threadCtx?.Dispose();
        }

        private bool InLegacySession()
        {
            return threadCtx != null;
        }

        private Guid InternalAcquire()
        {
            _inner.epoch.Resume();
            threadCtx.InitializeThread();
            Phase phase = _inner._systemState.phase;
            if (phase != Phase.REST)
            {
                throw new FasterException("Can acquire only in REST phase!");
            }
            Guid guid = Guid.NewGuid();
            threadCtx.Value = new FasterKV<Key, Value, Input, Output, Context>.FasterExecutionContext<Functions>(_functions);
            _inner.InitContext(threadCtx.Value, guid.ToString());

            threadCtx.Value.prevCtx = new FasterKV<Key, Value, Input, Output, Context>.FasterExecutionContext<Functions>(_functions);
            _inner.InitContext(threadCtx.Value.prevCtx, guid.ToString());
            threadCtx.Value.prevCtx.version--;
            _inner.InternalRefresh(threadCtx.Value);
            return guid;
        }

        private void InternalRelease(FasterKV<Key, Value, Input, Output, Context>.FasterExecutionContext<Functions> ctx)
        {
            Debug.Assert(ctx.HasNoPendingRequests);
            if (ctx.prevCtx != null)
            {
                Debug.Assert(ctx.prevCtx.HasNoPendingRequests);
            }
            Debug.Assert(ctx.phase == Phase.REST);

            _inner.epoch.Suspend();
        }
    }
}
