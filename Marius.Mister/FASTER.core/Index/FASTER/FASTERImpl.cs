﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162
#define CPR

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        internal enum LatchOperation : byte
        {
            None,
            Shared,
            Exclusive
        }

        #region Read Operation

        /// <summary>
        /// Read operation. Computes the 'output' from 'input' and current value corresponding to 'key'.
        /// When the read operation goes pending, once the record is retrieved from disk, InternalContinuePendingRead
        /// function is used to complete the operation.
        /// </summary>
        /// <param name="key">Key of the record.</param>
        /// <param name="input">Input required to compute output from value.</param>
        /// <param name="output">Location to store output computed from input and value.</param>
        /// <param name="userContext">User context for the operation, in case it goes pending.</param>
        /// <param name="pendingContext">Pending context used internally to store the context of the operation.</param>
        /// <param name="fasterSession">Callback functions.</param>
        /// <param name="sessionCtx">Session context</param>
        /// <param name="lsn">Operation serial number</param>
        /// <returns>
        /// <list type="table">
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>SUCCESS</term>
        ///     <term>The output has been computed using current value of 'key' and 'input'; and stored in 'output'.</term>
        ///     </item>
        ///     <item>
        ///     <term>RECORD_ON_DISK</term>
        ///     <term>The record corresponding to 'key' is on disk and the operation.</term>
        ///     </item>
        ///     <item>
        ///     <term>CPR_SHIFT_DETECTED</term>
        ///     <term>A shift in version has been detected. Synchronize immediately to avoid violating CPR consistency.</term>
        ///     </item>
        /// </list>
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus InternalRead<Input, Output, Context, Functions>(
                                    ref Key key,
                                    ref Input input,
                                    ref Output output,
                                    ref Context userContext,
                                    ref PendingContext<Input, Output, Context> pendingContext,
                                    Functions fasterSession,
                                    FasterExecutionContext<Input, Output, Context> sessionCtx,
                                    long lsn)
            where Functions : IFasterSession<Key, Value, Input, Output, Context>
        {
            var bucket = default(HashBucket*);
            var slot = default(int);
            var physicalAddress = default(long);
            var heldOperation = LatchOperation.None;

            var hash = comparer.GetHashCode64(ref key);
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

            if (sessionCtx.phase != Phase.REST)
                HeavyEnter(hash, sessionCtx, fasterSession);

            #region Trace back for record in in-memory HybridLog
            HashBucketEntry entry = default;
            var tagExists = FindTag(hash, tag, ref bucket, ref slot, ref entry);
            OperationStatus status;
            long logicalAddress;
            if (tagExists)
            {
                logicalAddress = entry.Address;

                if (UseReadCache && ReadFromCache(ref key, ref logicalAddress, ref physicalAddress))
                {
                    if (sessionCtx.phase == Phase.PREPARE && GetLatestRecordVersion(ref entry, sessionCtx.version) > sessionCtx.version)
                    {
                        status = OperationStatus.CPR_SHIFT_DETECTED;
                        goto CreatePendingContext; // Pivot thread
                    }
                    fasterSession.SingleReader(ref key, ref input, ref readcache.GetValue(physicalAddress), ref output);
                    return OperationStatus.SUCCESS;
                }

                if (logicalAddress >= hlog.HeadAddress)
                {
                    physicalAddress = hlog.GetPhysicalAddress(logicalAddress);

                    if (!comparer.Equals(ref key, ref hlog.GetKey(physicalAddress)))
                    {
                        logicalAddress = hlog.GetInfo(physicalAddress).PreviousAddress;
                        TraceBackForKeyMatch(ref key,
                                                logicalAddress,
                                                hlog.HeadAddress,
                                                out logicalAddress,
                                                out physicalAddress);
                    }
                }
            }
            else
            {
                // no tag found
                return OperationStatus.NOTFOUND;
            }
            #endregion

            if (sessionCtx.phase == Phase.PREPARE && GetLatestRecordVersion(ref entry, sessionCtx.version) > sessionCtx.version)
            {
                status = OperationStatus.CPR_SHIFT_DETECTED;
                goto CreatePendingContext; // Pivot thread
            }

            #region Normal processing

            // Mutable region (even fuzzy region is included here)
            if (logicalAddress >= hlog.SafeReadOnlyAddress)
            {
                if (hlog.GetInfo(physicalAddress).Tombstone)
                    return OperationStatus.NOTFOUND;

                fasterSession.ConcurrentReader(ref key, ref input, ref hlog.GetValue(physicalAddress), ref output);
                return OperationStatus.SUCCESS;
            }

            // Immutable region
            else if (logicalAddress >= hlog.HeadAddress)
            {
                if (hlog.GetInfo(physicalAddress).Tombstone)
                    return OperationStatus.NOTFOUND;

                fasterSession.SingleReader(ref key, ref input, ref hlog.GetValue(physicalAddress), ref output);
                return OperationStatus.SUCCESS;
            }

            // On-Disk Region
            else if (logicalAddress >= hlog.BeginAddress)
            {
                status = OperationStatus.RECORD_ON_DISK;

                if (sessionCtx.phase == Phase.PREPARE)
                {
                    Debug.Assert(heldOperation != LatchOperation.Exclusive);
                    if (heldOperation == LatchOperation.Shared || HashBucket.TryAcquireSharedLatch(bucket))
                    {
                        heldOperation = LatchOperation.Shared;
                    }
                    else
                    {
                        status = OperationStatus.CPR_SHIFT_DETECTED;
                    }

                    if (RelaxedCPR) // don't hold on to shared latched during IO
                    {
                        if (heldOperation == LatchOperation.Shared)
                            HashBucket.ReleaseSharedLatch(bucket);
                        heldOperation = LatchOperation.None;
                    }
                }

                goto CreatePendingContext;
            }

            // No record found
            else
            {
                return OperationStatus.NOTFOUND;
            }

        #endregion

        #region Create pending context
        CreatePendingContext:
            {
                pendingContext.type = OperationType.READ;
                pendingContext.key = hlog.GetKeyContainer(ref key);
                pendingContext.input = input;
                pendingContext.output = output;
                pendingContext.userContext = userContext;
                pendingContext.entry.word = entry.word;
                pendingContext.logicalAddress = logicalAddress;
                pendingContext.version = sessionCtx.version;
                pendingContext.serialNum = lsn;
                pendingContext.heldLatch = heldOperation;
            }
            #endregion

            return status;
        }
        #endregion

        #region Upsert Operation

        /// <summary>
        /// Upsert operation. Replaces the value corresponding to 'key' with provided 'value', if one exists 
        /// else inserts a new record with 'key' and 'value'.
        /// </summary>
        /// <param name="key">key of the record.</param>
        /// <param name="value">value to be updated to (or inserted if key does not exist).</param>
        /// <param name="userContext">User context for the operation, in case it goes pending.</param>
        /// <param name="pendingContext">Pending context used internally to store the context of the operation.</param>
        /// <param name="fasterSession">Callback functions.</param>
        /// <param name="sessionCtx">Session context</param>
        /// <param name="lsn">Operation serial number</param>
        /// <returns>
        /// <list type="table">
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>SUCCESS</term>
        ///     <term>The value has been successfully replaced(or inserted)</term>
        ///     </item>
        ///     <item>
        ///     <term>RETRY_LATER</term>
        ///     <term>Cannot  be processed immediately due to system state. Add to pending list and retry later</term>
        ///     </item>
        ///     <item>
        ///     <term>CPR_SHIFT_DETECTED</term>
        ///     <term>A shift in version has been detected. Synchronize immediately to avoid violating CPR consistency.</term>
        ///     </item>
        /// </list>
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus InternalUpsert<Input, Output, Context, FasterSession>(
                            ref Key key, ref Value value,
                            ref Context userContext,
                            ref PendingContext<Input, Output, Context> pendingContext,
                            FasterSession fasterSession,
                            FasterExecutionContext<Input, Output, Context> sessionCtx,
                            long lsn)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var status = default(OperationStatus);
            var bucket = default(HashBucket*);
            var slot = default(int);
            var logicalAddress = Constants.kInvalidAddress;
            var physicalAddress = default(long);
            var latchOperation = default(LatchOperation);

            var hash = comparer.GetHashCode64(ref key);
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

            if (sessionCtx.phase != Phase.REST)
                HeavyEnter(hash, sessionCtx, fasterSession);

            #region Trace back for record in in-memory HybridLog
            var entry = default(HashBucketEntry);
            FindOrCreateTag(hash, tag, ref bucket, ref slot, ref entry, hlog.BeginAddress);
            logicalAddress = entry.Address;

            if (UseReadCache)
                SkipAndInvalidateReadCache(ref logicalAddress, ref key);
            var latestLogicalAddress = logicalAddress;

            if (logicalAddress >= hlog.ReadOnlyAddress)
            {
                physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
                if (!comparer.Equals(ref key, ref hlog.GetKey(physicalAddress)))
                {
                    logicalAddress = hlog.GetInfo(physicalAddress).PreviousAddress;
                    TraceBackForKeyMatch(ref key,
                                        logicalAddress,
                                        hlog.ReadOnlyAddress,
                                        out logicalAddress,
                                        out physicalAddress);
                }
            }
            #endregion

            // Optimization for most common case
            if (sessionCtx.phase == Phase.REST && logicalAddress >= hlog.ReadOnlyAddress && !hlog.GetInfo(physicalAddress).Tombstone)
            {
                if (fasterSession.ConcurrentWriter(ref key, ref value, ref hlog.GetValue(physicalAddress)))
                {
                    return OperationStatus.SUCCESS;
                }
            }

            #region Entry latch operation
            if (sessionCtx.phase != Phase.REST)
            {
                switch (sessionCtx.phase)
                {
                    case Phase.PREPARE:
                        {
                            if (HashBucket.TryAcquireSharedLatch(bucket))
                            {
                                // Set to release shared latch (default)
                                latchOperation = LatchOperation.Shared;
                                if (GetLatestRecordVersion(ref entry, sessionCtx.version) > sessionCtx.version)
                                {
                                    status = OperationStatus.CPR_SHIFT_DETECTED;
                                    goto CreatePendingContext; // Pivot Thread
                                }
                                break; // Normal Processing
                            }
                            else
                            {
                                status = OperationStatus.CPR_SHIFT_DETECTED;
                                goto CreatePendingContext; // Pivot Thread
                            }
                        }
                    case Phase.IN_PROGRESS:
                        {
                            if (GetLatestRecordVersion(ref entry, sessionCtx.version) < sessionCtx.version)
                            {
                                if (HashBucket.TryAcquireExclusiveLatch(bucket))
                                {
                                    // Set to release exclusive latch (default)
                                    latchOperation = LatchOperation.Exclusive;
                                    goto CreateNewRecord; // Create a (v+1) record
                                }
                                else
                                {
                                    status = OperationStatus.RETRY_LATER;
                                    goto CreatePendingContext; // Go Pending
                                }
                            }
                            break; // Normal Processing
                        }
                    case Phase.WAIT_PENDING:
                        {
                            if (GetLatestRecordVersion(ref entry, sessionCtx.version) < sessionCtx.version)
                            {
                                if (HashBucket.NoSharedLatches(bucket))
                                {
                                    goto CreateNewRecord; // Create a (v+1) record
                                }
                                else
                                {
                                    status = OperationStatus.RETRY_LATER;
                                    goto CreatePendingContext; // Go Pending
                                }
                            }
                            break; // Normal Processing
                        }
                    case Phase.WAIT_FLUSH:
                        {
                            if (GetLatestRecordVersion(ref entry, sessionCtx.version) < sessionCtx.version)
                            {
                                goto CreateNewRecord; // Create a (v+1) record
                            }
                            break; // Normal Processing
                        }
                    default:
                        break;
                }
            }
            #endregion

            Debug.Assert(GetLatestRecordVersion(ref entry, sessionCtx.version) <= sessionCtx.version);

            #region Normal processing

            // Mutable Region: Update the record in-place
            if (logicalAddress >= hlog.ReadOnlyAddress && !hlog.GetInfo(physicalAddress).Tombstone)
            {
                if (fasterSession.ConcurrentWriter(ref key, ref value, ref hlog.GetValue(physicalAddress)))
                {
                    status = OperationStatus.SUCCESS;
                    goto LatchRelease; // Release shared latch (if acquired)
                }
            }

        // All other regions: Create a record in the mutable region
        #endregion

        #region Create new record in the mutable region
        CreateNewRecord:
            {
                // Immutable region or new record
                var recordSize = hlog.GetRecordSize(ref key, ref value);
                BlockAllocate(recordSize, out long newLogicalAddress, sessionCtx, fasterSession);
                var newPhysicalAddress = hlog.GetPhysicalAddress(newLogicalAddress);
                RecordInfo.WriteInfo(ref hlog.GetInfo(newPhysicalAddress),
                               sessionCtx.version,
                               true, false, false,
                               latestLogicalAddress);
                hlog.ShallowCopy(ref key, ref hlog.GetKey(newPhysicalAddress));
                fasterSession.SingleWriter(ref key, ref value,
                                       ref hlog.GetValue(newPhysicalAddress));

                var updatedEntry = default(HashBucketEntry);
                updatedEntry.Tag = tag;
                updatedEntry.Address = newLogicalAddress & Constants.kAddressMask;
                updatedEntry.Pending = entry.Pending;
                updatedEntry.Tentative = false;

                var foundEntry = default(HashBucketEntry);
                foundEntry.word = Interlocked.CompareExchange(
                                        ref bucket->bucket_entries[slot],
                                        updatedEntry.word, entry.word);

                if (foundEntry.word == entry.word)
                {
                    status = OperationStatus.SUCCESS;
                    goto LatchRelease;
                }
                else
                {
                    hlog.GetInfo(newPhysicalAddress).Invalid = true;
                    status = OperationStatus.RETRY_NOW;
                    goto LatchRelease;
                }
            }
        #endregion

        #region Create pending context
        CreatePendingContext:
            {
                pendingContext.type = OperationType.UPSERT;
                pendingContext.key = hlog.GetKeyContainer(ref key);
                pendingContext.value = hlog.GetValueContainer(ref value);
                pendingContext.userContext = userContext;
                pendingContext.entry.word = entry.word;
                pendingContext.logicalAddress = logicalAddress;
                pendingContext.version = sessionCtx.version;
                pendingContext.serialNum = lsn;
            }
        #endregion

        #region Latch release
        LatchRelease:
            {
                switch (latchOperation)
                {
                    case LatchOperation.Shared:
                        HashBucket.ReleaseSharedLatch(bucket);
                        break;
                    case LatchOperation.Exclusive:
                        HashBucket.ReleaseExclusiveLatch(bucket);
                        break;
                    default:
                        break;
                }
            }
            #endregion

            return status;
        }

        #endregion

        #region RMW Operation

        /// <summary>
        /// Read-Modify-Write Operation. Updates value of 'key' using 'input' and current value.
        /// Pending operations are processed either using InternalRetryPendingRMW or 
        /// InternalContinuePendingRMW.
        /// </summary>
        /// <param name="key">key of the record.</param>
        /// <param name="input">input used to update the value.</param>
        /// <param name="userContext">user context corresponding to operation used during completion callback.</param>
        /// <param name="pendingContext">pending context created when the operation goes pending.</param>
        /// <param name="fasterSession">Callback functions.</param>
        /// <param name="sessionCtx">Session context</param>
        /// <param name="lsn">Operation serial number</param>
        /// <returns>
        /// <list type="table">
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>SUCCESS</term>
        ///     <term>The value has been successfully updated(or inserted).</term>
        ///     </item>
        ///     <item>
        ///     <term>RECORD_ON_DISK</term>
        ///     <term>The record corresponding to 'key' is on disk. Issue async IO to retrieve record and retry later.</term>
        ///     </item>
        ///     <item>
        ///     <term>RETRY_LATER</term>
        ///     <term>Cannot  be processed immediately due to system state. Add to pending list and retry later.</term>
        ///     </item>
        ///     <item>
        ///     <term>CPR_SHIFT_DETECTED</term>
        ///     <term>A shift in version has been detected. Synchronize immediately to avoid violating CPR consistency.</term>
        ///     </item>
        /// </list>
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus InternalRMW<Input, Output, Context, FasterSession>(
                                   ref Key key, ref Input input,
                                   ref Context userContext,
                                   ref PendingContext<Input, Output, Context> pendingContext,
                                   FasterSession fasterSession,
                                   FasterExecutionContext<Input, Output, Context> sessionCtx,
                                   long lsn)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var recordSize = default(int);
            var bucket = default(HashBucket*);
            var slot = default(int);
            var logicalAddress = Constants.kInvalidAddress;
            var physicalAddress = default(long);
            var status = default(OperationStatus);
            var latchOperation = LatchOperation.None;
            var heldOperation = LatchOperation.None;

            var hash = comparer.GetHashCode64(ref key);
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

            if (sessionCtx.phase != Phase.REST)
                HeavyEnter(hash, sessionCtx, fasterSession);

            #region Trace back for record in in-memory HybridLog
            var entry = default(HashBucketEntry);
            FindOrCreateTag(hash, tag, ref bucket, ref slot, ref entry, hlog.BeginAddress);
            logicalAddress = entry.Address;

            // For simplicity, we don't let RMW operations use read cache
            if (UseReadCache)
                SkipReadCache(ref logicalAddress);
            var latestLogicalAddress = logicalAddress;

            if (logicalAddress >= hlog.HeadAddress)
            {
                physicalAddress = hlog.GetPhysicalAddress(logicalAddress);

                if (!comparer.Equals(ref key, ref hlog.GetKey(physicalAddress)))
                {
                    logicalAddress = hlog.GetInfo(physicalAddress).PreviousAddress;
                    TraceBackForKeyMatch(ref key, logicalAddress,
                                            hlog.HeadAddress,
                                            out logicalAddress,
                                            out physicalAddress);
                }
            }
            #endregion

            // Optimization for the most common case
            if (sessionCtx.phase == Phase.REST && logicalAddress >= hlog.ReadOnlyAddress && !hlog.GetInfo(physicalAddress).Tombstone)
            {
                if (fasterSession.InPlaceUpdater(ref key, ref input, ref hlog.GetValue(physicalAddress)))
                {
                    return OperationStatus.SUCCESS;
                }
            }

            #region Entry latch operation
            if (sessionCtx.phase != Phase.REST)
            {
                switch (sessionCtx.phase)
                {
                    case Phase.PREPARE:
                        {
                            Debug.Assert(pendingContext.heldLatch != LatchOperation.Exclusive);
                            if (pendingContext.heldLatch == LatchOperation.Shared || HashBucket.TryAcquireSharedLatch(bucket))
                            {
                                // Set to release shared latch (default)
                                latchOperation = LatchOperation.Shared;
                                if (GetLatestRecordVersion(ref entry, sessionCtx.version) > sessionCtx.version)
                                {
                                    status = OperationStatus.CPR_SHIFT_DETECTED;
                                    goto CreateFailureContext; // Pivot Thread
                                }
                                break; // Normal Processing
                            }
                            else
                            {
                                status = OperationStatus.CPR_SHIFT_DETECTED;
                                goto CreateFailureContext; // Pivot Thread
                            }
                        }
                    case Phase.IN_PROGRESS:
                        {
                            if (GetLatestRecordVersion(ref entry, sessionCtx.version) < sessionCtx.version)
                            {
                                Debug.Assert(pendingContext.heldLatch != LatchOperation.Shared);
                                if (pendingContext.heldLatch == LatchOperation.Exclusive || HashBucket.TryAcquireExclusiveLatch(bucket))
                                {
                                    // Set to release exclusive latch (default)
                                    latchOperation = LatchOperation.Exclusive;
                                    if (logicalAddress >= hlog.HeadAddress)
                                        goto CreateNewRecord; // Create a (v+1) record
                                }
                                else
                                {
                                    status = OperationStatus.RETRY_LATER;
                                    goto CreateFailureContext; // Go Pending
                                }
                            }
                            break; // Normal Processing
                        }
                    case Phase.WAIT_PENDING:
                        {
                            if (GetLatestRecordVersion(ref entry, sessionCtx.version) < sessionCtx.version)
                            {
                                if (HashBucket.NoSharedLatches(bucket))
                                {
                                    if (logicalAddress >= hlog.HeadAddress)
                                        goto CreateNewRecord; // Create a (v+1) record
                                }
                                else
                                {
                                    status = OperationStatus.RETRY_LATER;
                                    goto CreateFailureContext; // Go Pending
                                }
                            }
                            break; // Normal Processing
                        }
                    case Phase.WAIT_FLUSH:
                        {
                            if (GetLatestRecordVersion(ref entry, sessionCtx.version) < sessionCtx.version)
                            {
                                if (logicalAddress >= hlog.HeadAddress)
                                    goto CreateNewRecord; // Create a (v+1) record
                            }
                            break; // Normal Processing
                        }
                    default:
                        break;
                }
            }
            #endregion

            Debug.Assert(GetLatestRecordVersion(ref entry, sessionCtx.version) <= sessionCtx.version);

            #region Normal processing

            // Mutable Region: Update the record in-place
            if (logicalAddress >= hlog.ReadOnlyAddress && !hlog.GetInfo(physicalAddress).Tombstone)
            {
                if (FoldOverSnapshot)
                {
                    Debug.Assert(hlog.GetInfo(physicalAddress).Version == sessionCtx.version);
                }

                if (fasterSession.InPlaceUpdater(ref key, ref input, ref hlog.GetValue(physicalAddress)))
                {
                    status = OperationStatus.SUCCESS;
                    goto LatchRelease; // Release shared latch (if acquired)
                }
            }

            // Fuzzy Region: Must go pending due to lost-update anomaly
            else if (logicalAddress >= hlog.SafeReadOnlyAddress && !hlog.GetInfo(physicalAddress).Tombstone)
            {
                status = OperationStatus.RETRY_LATER;
                // Do not retain latch for pendings ops in relaxed CPR
                if (!RelaxedCPR)
                {
                    // Retain the shared latch (if acquired)
                    if (latchOperation == LatchOperation.Shared)
                    {
                        heldOperation = latchOperation;
                        latchOperation = LatchOperation.None;
                    }
                }
                goto CreateFailureContext; // Go pending
            }

            // Safe Read-Only Region: Create a record in the mutable region
            else if (logicalAddress >= hlog.HeadAddress)
            {
                goto CreateNewRecord;
            }

            // Disk Region: Need to issue async io requests
            else if (logicalAddress >= hlog.BeginAddress)
            {
                status = OperationStatus.RECORD_ON_DISK;
                // Do not retain latch for pendings ops in relaxed CPR
                if (!RelaxedCPR)
                {
                    // Retain the shared latch (if acquired)
                    if (latchOperation == LatchOperation.Shared)
                    {
                        heldOperation = latchOperation;
                        latchOperation = LatchOperation.None;
                    }
                }
                goto CreateFailureContext; // Go pending
            }

            // No record exists - create new
            else
            {
                goto CreateNewRecord;
            }

        #endregion

        #region Create new record
        CreateNewRecord:
            {
                recordSize = (logicalAddress < hlog.BeginAddress) ?
                                hlog.GetInitialRecordSize(ref key, ref input, fasterSession) :
                                hlog.GetRecordSize(physicalAddress, ref input, fasterSession);
                BlockAllocate(recordSize, out long newLogicalAddress, sessionCtx, fasterSession);
                var newPhysicalAddress = hlog.GetPhysicalAddress(newLogicalAddress);
                RecordInfo.WriteInfo(ref hlog.GetInfo(newPhysicalAddress), sessionCtx.version,
                               true, false, false,
                               latestLogicalAddress);
                hlog.ShallowCopy(ref key, ref hlog.GetKey(newPhysicalAddress));
                if (logicalAddress < hlog.BeginAddress)
                {
                    fasterSession.InitialUpdater(ref key, ref input, ref hlog.GetValue(newPhysicalAddress));
                    status = OperationStatus.NOTFOUND;
                }
                else if (logicalAddress >= hlog.HeadAddress)
                {
                    if (hlog.GetInfo(physicalAddress).Tombstone)
                    {
                        fasterSession.InitialUpdater(ref key, ref input, ref hlog.GetValue(newPhysicalAddress));
                        status = OperationStatus.NOTFOUND;
                    }
                    else
                    {
                        fasterSession.CopyUpdater(ref key, ref input,
                                                ref hlog.GetValue(physicalAddress),
                                                ref hlog.GetValue(newPhysicalAddress));
                        status = OperationStatus.SUCCESS;
                    }
                }
                else
                {
                    // ah, old record slipped onto disk
                    hlog.GetInfo(newPhysicalAddress).Invalid = true;
                    status = OperationStatus.RETRY_NOW;
                    goto LatchRelease;
                }

                var updatedEntry = default(HashBucketEntry);
                updatedEntry.Tag = tag;
                updatedEntry.Address = newLogicalAddress & Constants.kAddressMask;
                updatedEntry.Pending = entry.Pending;
                updatedEntry.Tentative = false;

                var foundEntry = default(HashBucketEntry);
                foundEntry.word = Interlocked.CompareExchange(
                                        ref bucket->bucket_entries[slot],
                                        updatedEntry.word, entry.word);

                if (foundEntry.word == entry.word)
                {
                    goto LatchRelease;
                }
                else
                {
                    // CAS failed
                    hlog.GetInfo(newPhysicalAddress).Invalid = true;
                    status = OperationStatus.RETRY_NOW;
                    goto LatchRelease;
                }
            }
        #endregion

        #region Create failure context
        CreateFailureContext:
            {
                pendingContext.type = OperationType.RMW;
                pendingContext.key = hlog.GetKeyContainer(ref key);
                pendingContext.input = input;
                pendingContext.userContext = userContext;
                pendingContext.entry.word = entry.word;
                pendingContext.logicalAddress = logicalAddress;
                pendingContext.version = sessionCtx.version;
                pendingContext.serialNum = lsn;
                pendingContext.heldLatch = heldOperation;
            }
        #endregion

        #region Latch release
        LatchRelease:
            {
                switch (latchOperation)
                {
                    case LatchOperation.Shared:
                        HashBucket.ReleaseSharedLatch(bucket);
                        break;
                    case LatchOperation.Exclusive:
                        HashBucket.ReleaseExclusiveLatch(bucket);
                        break;
                    default:
                        break;
                }
            }
            #endregion

            return status;
        }

        #endregion

        #region Delete Operation

        /// <summary>
        /// Delete operation. Replaces the value corresponding to 'key' with tombstone.
        /// If at head, tries to remove item from hash chain
        /// </summary>
        /// <param name="key">Key of the record to be deleted.</param>
        /// <param name="userContext">User context for the operation, in case it goes pending.</param>
        /// <param name="pendingContext">Pending context used internally to store the context of the operation.</param>
        /// <param name="fasterSession">Callback functions.</param>
        /// <param name="sessionCtx">Session context</param>
        /// <param name="lsn">Operation serial number</param>
        /// <returns>
        /// <list type="table">
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>SUCCESS</term>
        ///     <term>The value has been successfully deleted</term>
        ///     </item>
        ///     <item>
        ///     <term>RETRY_LATER</term>
        ///     <term>Cannot  be processed immediately due to system state. Add to pending list and retry later</term>
        ///     </item>
        ///     <item>
        ///     <term>CPR_SHIFT_DETECTED</term>
        ///     <term>A shift in version has been detected. Synchronize immediately to avoid violating CPR consistency.</term>
        ///     </item>
        /// </list>
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus InternalDelete<Input, Output, Context, FasterSession>(
                            ref Key key,
                            ref Context userContext,
                            ref PendingContext<Input, Output, Context> pendingContext,
                            FasterSession fasterSession,
                            FasterExecutionContext<Input, Output, Context> sessionCtx,
                            long lsn)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var status = default(OperationStatus);
            var bucket = default(HashBucket*);
            var slot = default(int);
            var logicalAddress = Constants.kInvalidAddress;
            var physicalAddress = default(long);
            var latchOperation = default(LatchOperation);

            var hash = comparer.GetHashCode64(ref key);
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

            if (sessionCtx.phase != Phase.REST)
                HeavyEnter(hash, sessionCtx, fasterSession);

            #region Trace back for record in in-memory HybridLog
            var entry = default(HashBucketEntry);
            var tagExists = FindTag(hash, tag, ref bucket, ref slot, ref entry);
            if (!tagExists)
                return OperationStatus.NOTFOUND;

            logicalAddress = entry.Address;

            if (UseReadCache)
                SkipAndInvalidateReadCache(ref logicalAddress, ref key);
            var latestLogicalAddress = logicalAddress;

            if (logicalAddress >= hlog.ReadOnlyAddress)
            {
                physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
                if (!comparer.Equals(ref key, ref hlog.GetKey(physicalAddress)))
                {
                    logicalAddress = hlog.GetInfo(physicalAddress).PreviousAddress;
                    TraceBackForKeyMatch(ref key,
                                        logicalAddress,
                                        hlog.ReadOnlyAddress,
                                        out logicalAddress,
                                        out physicalAddress);
                }
            }
            #endregion

            #region Entry latch operation
            if (sessionCtx.phase != Phase.REST)
            {
                switch (sessionCtx.phase)
                {
                    case Phase.PREPARE:
                        {
                            if (HashBucket.TryAcquireSharedLatch(bucket))
                            {
                                // Set to release shared latch (default)
                                latchOperation = LatchOperation.Shared;
                                if (GetLatestRecordVersion(ref entry, sessionCtx.version) > sessionCtx.version)
                                {
                                    status = OperationStatus.CPR_SHIFT_DETECTED;
                                    goto CreatePendingContext; // Pivot Thread
                                }
                                break; // Normal Processing
                            }
                            else
                            {
                                status = OperationStatus.CPR_SHIFT_DETECTED;
                                goto CreatePendingContext; // Pivot Thread
                            }
                        }
                    case Phase.IN_PROGRESS:
                        {
                            if (GetLatestRecordVersion(ref entry, sessionCtx.version) < sessionCtx.version)
                            {
                                if (HashBucket.TryAcquireExclusiveLatch(bucket))
                                {
                                    // Set to release exclusive latch (default)
                                    latchOperation = LatchOperation.Exclusive;
                                    goto CreateNewRecord; // Create a (v+1) record
                                }
                                else
                                {
                                    status = OperationStatus.RETRY_LATER;
                                    goto CreatePendingContext; // Go Pending
                                }
                            }
                            break; // Normal Processing
                        }
                    case Phase.WAIT_PENDING:
                        {
                            if (GetLatestRecordVersion(ref entry, sessionCtx.version) < sessionCtx.version)
                            {
                                if (HashBucket.NoSharedLatches(bucket))
                                {
                                    goto CreateNewRecord; // Create a (v+1) record
                                }
                                else
                                {
                                    status = OperationStatus.RETRY_LATER;
                                    goto CreatePendingContext; // Go Pending
                                }
                            }
                            break; // Normal Processing
                        }
                    case Phase.WAIT_FLUSH:
                        {
                            if (GetLatestRecordVersion(ref entry, sessionCtx.version) < sessionCtx.version)
                            {
                                goto CreateNewRecord; // Create a (v+1) record
                            }
                            break; // Normal Processing
                        }
                    default:
                        break;
                }
            }
            #endregion

            Debug.Assert(GetLatestRecordVersion(ref entry, sessionCtx.version) <= sessionCtx.version);

            #region Normal processing

            // Mutable Region: Update the record in-place
            if (logicalAddress >= hlog.ReadOnlyAddress)
            {
                // Apply tombstone bit to the record
                hlog.GetInfo(physicalAddress).Tombstone = true;

                if (WriteDefaultOnDelete)
                {
                    // Write default value. Ignore return value; the record is already marked
                    Value v = default;
                    fasterSession.ConcurrentWriter(ref hlog.GetKey(physicalAddress), ref v, ref hlog.GetValue(physicalAddress));
                }

                // Try to update hash chain and completely elide record only if previous address points to invalid address
                if (entry.Address == logicalAddress && hlog.GetInfo(physicalAddress).PreviousAddress < hlog.BeginAddress)
                {
                    var updatedEntry = default(HashBucketEntry);
                    updatedEntry.Tag = 0;
                    if (hlog.GetInfo(physicalAddress).PreviousAddress == Constants.kTempInvalidAddress)
                        updatedEntry.Address = Constants.kInvalidAddress;
                    else
                        updatedEntry.Address = hlog.GetInfo(physicalAddress).PreviousAddress;
                    updatedEntry.Pending = entry.Pending;
                    updatedEntry.Tentative = false;

                    // Ignore return value; this is a performance optimization to keep the hash table clean if we can
                    Interlocked.CompareExchange(ref bucket->bucket_entries[slot], updatedEntry.word, entry.word);
                }

                status = OperationStatus.SUCCESS;
                goto LatchRelease; // Release shared latch (if acquired)
            }

            // All other regions: Create a record in the mutable region
            #endregion

            #region Create new record in the mutable region
            CreateNewRecord:
            {
                var value = default(Value);
                // Immutable region or new record
                // Allocate default record size for tombstone
                var recordSize = hlog.GetRecordSize(ref key, ref value);
                BlockAllocate(recordSize, out long newLogicalAddress, sessionCtx, fasterSession);
                var newPhysicalAddress = hlog.GetPhysicalAddress(newLogicalAddress);
                RecordInfo.WriteInfo(ref hlog.GetInfo(newPhysicalAddress),
                               sessionCtx.version,
                               true, true, false,
                               latestLogicalAddress);
                hlog.ShallowCopy(ref key, ref hlog.GetKey(newPhysicalAddress));

                var updatedEntry = default(HashBucketEntry);
                updatedEntry.Tag = tag;
                updatedEntry.Address = newLogicalAddress & Constants.kAddressMask;
                updatedEntry.Pending = entry.Pending;
                updatedEntry.Tentative = false;

                var foundEntry = default(HashBucketEntry);
                foundEntry.word = Interlocked.CompareExchange(
                                        ref bucket->bucket_entries[slot],
                                        updatedEntry.word, entry.word);

                if (foundEntry.word == entry.word)
                {
                    status = OperationStatus.SUCCESS;
                    goto LatchRelease;
                }
                else
                {
                    hlog.GetInfo(newPhysicalAddress).Invalid = true;
                    status = OperationStatus.RETRY_NOW;
                    goto LatchRelease;
                }
            }
        #endregion

        #region Create pending context
        CreatePendingContext:
            {
                pendingContext.type = OperationType.DELETE;
                pendingContext.key = hlog.GetKeyContainer(ref key);
                pendingContext.userContext = userContext;
                pendingContext.entry.word = entry.word;
                pendingContext.logicalAddress = logicalAddress;
                pendingContext.version = sessionCtx.version;
                pendingContext.serialNum = lsn;
            }
        #endregion

        #region Latch release
        LatchRelease:
            {
                switch (latchOperation)
                {
                    case LatchOperation.Shared:
                        HashBucket.ReleaseSharedLatch(bucket);
                        break;
                    case LatchOperation.Exclusive:
                        HashBucket.ReleaseExclusiveLatch(bucket);
                        break;
                    default:
                        break;
                }
            }
            #endregion

            return status;
        }

        #endregion

        #region ContainsKeyInMemory

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status InternalContainsKeyInMemory<Input, Output, Context, FasterSession>(
            ref Key key, 
            FasterExecutionContext<Input, Output, Context> sessionCtx, 
            FasterSession fasterSession, 
            long fromAddress = -1)
            where FasterSession : IFasterSession
        {
            if (fromAddress == -1)
                fromAddress = hlog.HeadAddress;
            else
                Debug.Assert(fromAddress >= hlog.HeadAddress);

            var bucket = default(HashBucket*);
            var slot = default(int);
            long physicalAddress;

            var hash = comparer.GetHashCode64(ref key);
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

            if (sessionCtx.phase != Phase.REST)
                HeavyEnter(hash, sessionCtx, fasterSession);

            HashBucketEntry entry = default;
            var tagExists = FindTag(hash, tag, ref bucket, ref slot, ref entry);

            if (tagExists)
            {
                long logicalAddress = entry.Address;

                if (UseReadCache)
                    SkipReadCache(ref logicalAddress);

                if (logicalAddress >= fromAddress)
                {
                    physicalAddress = hlog.GetPhysicalAddress(logicalAddress);

                    if (!comparer.Equals(ref key, ref hlog.GetKey(physicalAddress)))
                    {
                        logicalAddress = hlog.GetInfo(physicalAddress).PreviousAddress;
                        TraceBackForKeyMatch(ref key,
                                                logicalAddress,
                                                fromAddress,
                                                out logicalAddress,
                                                out _);
                    }

                    if (logicalAddress < fromAddress)
                        return Status.NOTFOUND;
                    else
                        return Status.OK;
                }
                else
                    return Status.NOTFOUND;
            }
            else
            {
                // no tag found
                return Status.NOTFOUND;
            }
        }
        #endregion

        #region Continue operations
        /// <summary>
        /// Continue a pending read operation. Computes 'output' from 'input' and value corresponding to 'key'
        /// obtained from disk. Optionally, it copies the value to tail to serve future read/write requests quickly.
        /// </summary>
        /// <param name="ctx">The thread (or session) context to execute operation in.</param>
        /// <param name="request">Async response from disk.</param>
        /// <param name="pendingContext">Pending context corresponding to operation.</param>
        /// <param name="fasterSession">Callback functions.</param>
        /// <param name="currentCtx"></param>
        /// <returns>
        /// <list type = "table" >
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>SUCCESS</term>
        ///     <term>The output has been computed and stored in 'output'.</term>
        ///     </item>
        /// </list>
        /// </returns>
        internal OperationStatus InternalContinuePendingRead<Input, Output, Context, FasterSession>(
                            FasterExecutionContext<Input, Output, Context> ctx,
                            AsyncIOContext<Key, Value> request,
                            ref PendingContext<Input, Output, Context> pendingContext,
                            FasterSession fasterSession,
                            FasterExecutionContext<Input, Output, Context> currentCtx)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            Debug.Assert(RelaxedCPR || pendingContext.version == ctx.version);

            if (request.logicalAddress >= hlog.BeginAddress)
            {
                Debug.Assert(hlog.GetInfoFromBytePointer(request.record.GetValidPointer()).Version <= ctx.version);

                if (hlog.GetInfoFromBytePointer(request.record.GetValidPointer()).Tombstone)
                    return OperationStatus.NOTFOUND;

                fasterSession.SingleReader(ref pendingContext.key.Get(), ref pendingContext.input,
                                       ref hlog.GetContextRecordValue(ref request), ref pendingContext.output);

                if (CopyReadsToTail || UseReadCache)
                {
                    InternalContinuePendingReadCopyToTail(ctx, request, ref pendingContext, fasterSession, currentCtx);
                }
            }
            else
                return OperationStatus.NOTFOUND;

            return OperationStatus.SUCCESS;
        }

        /// <summary>
        /// Copies the record read from disk to tail of the HybridLog. 
        /// </summary>
        /// <param name="opCtx"> The thread(or session) context to execute operation in.</param>
        /// <param name="request">Async response from disk.</param>
        /// <param name="pendingContext">Pending context corresponding to operation.</param>
        /// <param name="fasterSession">Callback functions.</param>
        /// <param name="currentCtx"></param>
        internal void InternalContinuePendingReadCopyToTail<Input, Output, Context, FasterSession>(
                                    FasterExecutionContext<Input, Output, Context> opCtx,
                                    AsyncIOContext<Key, Value> request,
                                    ref PendingContext<Input, Output, Context> pendingContext,
                                    FasterSession fasterSession,
                                    FasterExecutionContext<Input, Output, Context> currentCtx)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            Debug.Assert(RelaxedCPR || pendingContext.version == opCtx.version);

            var recordSize = default(int);
            var bucket = default(HashBucket*);
            var slot = default(int);
            var logicalAddress = Constants.kInvalidAddress;
            var physicalAddress = default(long);

            var hash = comparer.GetHashCode64(ref pendingContext.key.Get());
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

            #region Trace back record in in-memory HybridLog
            var entry = default(HashBucketEntry);
            FindOrCreateTag(hash, tag, ref bucket, ref slot, ref entry, hlog.BeginAddress);
            logicalAddress = entry.word & Constants.kAddressMask;

            if (UseReadCache)
                SkipReadCache(ref logicalAddress);
            var latestLogicalAddress = logicalAddress;

            if (logicalAddress >= hlog.HeadAddress)
            {
                physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
                if (!comparer.Equals(ref pendingContext.key.Get(), ref hlog.GetKey(physicalAddress)))
                {
                    logicalAddress = hlog.GetInfo(physicalAddress).PreviousAddress;
                    TraceBackForKeyMatch(ref pendingContext.key.Get(),
                                            logicalAddress,
                                            hlog.HeadAddress,
                                            out logicalAddress,
                                            out physicalAddress);
                }
            }
            #endregion

            if (logicalAddress > pendingContext.entry.Address)
            {
                // Give up early
                return;
            }

            #region Create new copy in mutable region
            physicalAddress = (long)request.record.GetValidPointer();
            recordSize = hlog.GetRecordSize(physicalAddress);

            long newLogicalAddress, newPhysicalAddress;
            if (UseReadCache)
            {
                BlockAllocateReadCache(recordSize, out newLogicalAddress, currentCtx, fasterSession);
                newPhysicalAddress = readcache.GetPhysicalAddress(newLogicalAddress);
                RecordInfo.WriteInfo(ref readcache.GetInfo(newPhysicalAddress), opCtx.version,
                                    true, false, false,
                                    entry.Address);
                readcache.ShallowCopy(ref pendingContext.key.Get(), ref readcache.GetKey(newPhysicalAddress));
                fasterSession.SingleWriter(ref pendingContext.key.Get(),
                                       ref hlog.GetContextRecordValue(ref request),
                                       ref readcache.GetValue(newPhysicalAddress));
            }
            else
            {
                BlockAllocate(recordSize, out newLogicalAddress, currentCtx, fasterSession);
                newPhysicalAddress = hlog.GetPhysicalAddress(newLogicalAddress);
                RecordInfo.WriteInfo(ref hlog.GetInfo(newPhysicalAddress), opCtx.version,
                               true, false, false,
                               latestLogicalAddress);
                hlog.ShallowCopy(ref pendingContext.key.Get(), ref hlog.GetKey(newPhysicalAddress));
                fasterSession.SingleWriter(ref pendingContext.key.Get(),
                                       ref hlog.GetContextRecordValue(ref request),
                                       ref hlog.GetValue(newPhysicalAddress));
            }


            var updatedEntry = default(HashBucketEntry);
            updatedEntry.Tag = tag;
            updatedEntry.Address = newLogicalAddress & Constants.kAddressMask;
            updatedEntry.Pending = entry.Pending;
            updatedEntry.Tentative = false;
            updatedEntry.ReadCache = UseReadCache;

            var foundEntry = default(HashBucketEntry);
            foundEntry.word = Interlocked.CompareExchange(
                                            ref bucket->bucket_entries[slot],
                                            updatedEntry.word,
                                            entry.word);
            if (foundEntry.word != entry.word)
            {
                if (!UseReadCache) hlog.GetInfo(newPhysicalAddress).Invalid = true;
                // We don't retry, just give up
            }
            #endregion
        }

        /// <summary>
        /// Continue a pending RMW operation with the record retrieved from disk.
        /// </summary>
        /// <param name="opCtx">thread (or session) context under which operation must be executed.</param>
        /// <param name="request">record read from the disk.</param>
        /// <param name="pendingContext">internal context for the pending RMW operation</param>
        /// <param name="fasterSession">Callback functions.</param>
        /// <param name="sessionCtx">Session context</param>
        /// <returns>
        /// <list type="table">
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>SUCCESS</term>
        ///     <term>The value has been successfully updated(or inserted).</term>
        ///     </item>
        ///     <item>
        ///     <term>RECORD_ON_DISK</term>
        ///     <term>The record corresponding to 'key' is on disk. Issue async IO to retrieve record and retry later.</term>
        ///     </item>
        ///     <item>
        ///     <term>RETRY_LATER</term>
        ///     <term>Cannot  be processed immediately due to system state. Add to pending list and retry later.</term>
        ///     </item>
        /// </list>
        /// </returns>
        internal OperationStatus InternalContinuePendingRMW<Input, Output, Context, FasterSession>(
                                    FasterExecutionContext<Input, Output, Context> opCtx,
                                    AsyncIOContext<Key, Value> request,
                                    ref PendingContext<Input, Output, Context> pendingContext,
                                    FasterSession fasterSession,
                                    FasterExecutionContext<Input, Output, Context> sessionCtx)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var recordSize = default(int);
            var bucket = default(HashBucket*);
            var slot = default(int);
            var logicalAddress = Constants.kInvalidAddress;
            var physicalAddress = default(long);
            var status = default(OperationStatus);
            ref Key key = ref pendingContext.key.Get();

            var hash = comparer.GetHashCode64(ref key);
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

            while (true)
            {
                #region Trace Back for Record on In-Memory HybridLog
                var entry = default(HashBucketEntry);
                FindOrCreateTag(hash, tag, ref bucket, ref slot, ref entry, hlog.BeginAddress);
                logicalAddress = entry.Address;

                // For simplicity, we don't let RMW operations use read cache
                if (UseReadCache)
                    SkipReadCache(ref logicalAddress);
                var latestLogicalAddress = logicalAddress;

                if (logicalAddress >= hlog.HeadAddress)
                {
                    physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
                    if (!comparer.Equals(ref key, ref hlog.GetKey(physicalAddress)))
                    {
                        logicalAddress = hlog.GetInfo(physicalAddress).PreviousAddress;
                        TraceBackForKeyMatch(ref key,
                                                logicalAddress,
                                                hlog.HeadAddress,
                                                out logicalAddress,
                                                out physicalAddress);
                    }
                }
                #endregion

                var previousFirstRecordAddress = pendingContext.entry.Address;
                if (logicalAddress > previousFirstRecordAddress)
                {
                    break;
                }

                #region Create record in mutable region
                if ((request.logicalAddress < hlog.BeginAddress) || (hlog.GetInfoFromBytePointer(request.record.GetValidPointer()).Tombstone))
                {
                    recordSize = hlog.GetInitialRecordSize(ref key, ref pendingContext.input, fasterSession);
                }
                else
                {
                    physicalAddress = (long)request.record.GetValidPointer();
                    recordSize = hlog.GetRecordSize(physicalAddress, ref pendingContext.input, fasterSession);
                }
                BlockAllocate(recordSize, out long newLogicalAddress, sessionCtx, fasterSession);
                var newPhysicalAddress = hlog.GetPhysicalAddress(newLogicalAddress);
                RecordInfo.WriteInfo(ref hlog.GetInfo(newPhysicalAddress), opCtx.version,
                               true, false, false,
                               latestLogicalAddress);
                hlog.ShallowCopy(ref key, ref hlog.GetKey(newPhysicalAddress));
                if ((request.logicalAddress < hlog.BeginAddress) || (hlog.GetInfoFromBytePointer(request.record.GetValidPointer()).Tombstone))
                {
                    fasterSession.InitialUpdater(ref key,
                                             ref pendingContext.input,
                                             ref hlog.GetValue(newPhysicalAddress));
                    status = OperationStatus.NOTFOUND;
                }
                else
                {
                    fasterSession.CopyUpdater(ref key,
                                          ref pendingContext.input,
                                          ref hlog.GetContextRecordValue(ref request),
                                          ref hlog.GetValue(newPhysicalAddress));
                    status = OperationStatus.SUCCESS;
                }

                var updatedEntry = default(HashBucketEntry);
                updatedEntry.Tag = tag;
                updatedEntry.Address = newLogicalAddress & Constants.kAddressMask;
                updatedEntry.Pending = entry.Pending;
                updatedEntry.Tentative = false;

                var foundEntry = default(HashBucketEntry);
                foundEntry.word = Interlocked.CompareExchange(
                                            ref bucket->bucket_entries[slot],
                                            updatedEntry.word, entry.word);

                if (foundEntry.word == entry.word)
                {
                    return status;
                }
                else
                {
                    hlog.GetInfo(newPhysicalAddress).Invalid = true;
                }
                #endregion
            }

            OperationStatus internalStatus;
            do
                internalStatus = InternalRMW(ref pendingContext.key.Get(), ref pendingContext.input, ref pendingContext.userContext, ref pendingContext, fasterSession, opCtx, pendingContext.serialNum);
            while (internalStatus == OperationStatus.RETRY_NOW);
            return internalStatus;
        }

        #endregion

        #region Helper Functions

        /// <summary>
        /// Performs appropriate handling based on the internal failure status of the trial.
        /// </summary>
        /// <param name="opCtx">Thread (or session) context under which operation was tried to execute.</param>
        /// <param name="currentCtx">Current context</param>
        /// <param name="pendingContext">Internal context of the operation.</param>
        /// <param name="fasterSession">Callback functions.</param>
        /// <param name="status">Internal status of the trial.</param>
        /// <param name="asyncOp">When operation issued via async call</param>
        /// <param name="request">IO request, if operation went pending</param>
        /// <returns>
        /// <list type="table">
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>OK</term>
        ///     <term>The operation has been completed successfully.</term>
        ///     </item>
        ///     <item>
        ///     <term>PENDING</term>
        ///     <term>The operation is still pending and will callback when done.</term>
        ///     </item>
        /// </list>
        /// </returns>
        internal Status HandleOperationStatus<Input, Output, Context, FasterSession>(
            FasterExecutionContext<Input, Output, Context> opCtx,
            FasterExecutionContext<Input, Output, Context> currentCtx,
            ref PendingContext<Input, Output, Context> pendingContext,
            FasterSession fasterSession,
            OperationStatus status, bool asyncOp, out AsyncIOContext<Key, Value> request)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            request = default;

            if (status == OperationStatus.CPR_SHIFT_DETECTED)
            {
                SynchronizeEpoch(opCtx, currentCtx, ref pendingContext, fasterSession);
            }

            if (status == OperationStatus.CPR_SHIFT_DETECTED || ((asyncOp || RelaxedCPR) && status == OperationStatus.RETRY_LATER))
            {
                #region Retry as (v+1) Operation
                var internalStatus = default(OperationStatus);
                do
                {
                    switch (pendingContext.type)
                    {
                        case OperationType.READ:
                            internalStatus = InternalRead(ref pendingContext.key.Get(),
                                                          ref pendingContext.input,
                                                          ref pendingContext.output,
                                                          ref pendingContext.userContext,
                                                          ref pendingContext, fasterSession, currentCtx, pendingContext.serialNum);
                            break;
                        case OperationType.UPSERT:
                            internalStatus = InternalUpsert(ref pendingContext.key.Get(),
                                                            ref pendingContext.value.Get(),
                                                            ref pendingContext.userContext,
                                                            ref pendingContext, fasterSession, currentCtx, pendingContext.serialNum);
                            break;
                        case OperationType.DELETE:
                            internalStatus = InternalDelete(ref pendingContext.key.Get(),
                                                            ref pendingContext.userContext,
                                                            ref pendingContext, fasterSession, currentCtx, pendingContext.serialNum);
                            break;
                        case OperationType.RMW:
                            internalStatus = InternalRMW(ref pendingContext.key.Get(),
                                                         ref pendingContext.input,
                                                         ref pendingContext.userContext,
                                                         ref pendingContext, fasterSession, currentCtx, pendingContext.serialNum);
                            break;
                    }
                    Debug.Assert(internalStatus != OperationStatus.CPR_SHIFT_DETECTED);
                } while (internalStatus == OperationStatus.RETRY_NOW || ((asyncOp || RelaxedCPR) && internalStatus == OperationStatus.RETRY_LATER));
                // Note that we spin in case of { async op + strict CPR } which is fine as this combination is rare/discouraged

                status = internalStatus;
                #endregion
            }

            if (status == OperationStatus.SUCCESS || status == OperationStatus.NOTFOUND)
            {
                return (Status)status;
            }
            else if (status == OperationStatus.RECORD_ON_DISK)
            {
                //Add context to dictionary
                pendingContext.id = opCtx.totalPending++;
                opCtx.ioPendingRequests.Add(pendingContext.id, pendingContext);

                // Issue asynchronous I/O request
                request.id = pendingContext.id;
                request.request_key = pendingContext.key;
                request.logicalAddress = pendingContext.logicalAddress;
                request.record = default;
                if (asyncOp)
                    request.asyncOperation = new TaskCompletionSource<AsyncIOContext<Key, Value>>(TaskCreationOptions.RunContinuationsAsynchronously);
                else
                    request.callbackQueue = opCtx.readyResponses;
                
                hlog.AsyncGetFromDisk(pendingContext.logicalAddress,
                                 hlog.GetAverageRecordSize(),
                                 request);

                return Status.PENDING;
            }
            else if (status == OperationStatus.RETRY_LATER)
            {
                Debug.Assert(!asyncOp);
                opCtx.retryRequests.Enqueue(pendingContext);
                return Status.PENDING;
            }
            else
            {
                return Status.ERROR;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SynchronizeEpoch<Input, Output, Context, FasterSession>(
            FasterExecutionContext<Input, Output, Context> opCtx, 
            FasterExecutionContext<Input, Output, Context> currentCtx, 
            ref PendingContext<Input, Output, Context> pendingContext, 
            FasterSession fasterSession)
            where FasterSession : IFasterSession
        {
            var version = opCtx.version;
            Debug.Assert(currentCtx.version == version);
            Debug.Assert(currentCtx.phase == Phase.PREPARE);
            InternalRefresh(currentCtx, fasterSession);
            Debug.Assert(currentCtx.version > version);

            pendingContext.version = currentCtx.version;
        }

        private void AcquireSharedLatch(Key key)
        {
            var bucket = default(HashBucket*);
            var slot = default(int);
            var hash = comparer.GetHashCode64(ref key);
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);
            var entry = default(HashBucketEntry);
            FindOrCreateTag(hash, tag, ref bucket, ref slot, ref entry, hlog.BeginAddress);
            HashBucket.TryAcquireSharedLatch(bucket);
        }

        private void ReleaseSharedLatch(Key key)
        {
            var bucket = default(HashBucket*);
            var slot = default(int);
            var hash = comparer.GetHashCode64(ref key);
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);
            var entry = default(HashBucketEntry);
            FindOrCreateTag(hash, tag, ref bucket, ref slot, ref entry, hlog.BeginAddress);
            HashBucket.ReleaseSharedLatch(bucket);
        }

        private void HeavyEnter<Input, Output, Context, FasterSession>(long hash, FasterExecutionContext<Input, Output, Context> ctx, FasterSession session)
            where FasterSession : IFasterSession
        {
            if (ctx.phase == Phase.PREPARE_GROW)
            {
                // We spin-wait as a simplification
                // Could instead do a "heavy operation" here
                while (systemState.phase != Phase.IN_PROGRESS_GROW)
                    Thread.SpinWait(100);
                InternalRefresh(ctx, session);
            }
            if (ctx.phase == Phase.IN_PROGRESS_GROW)
            {
                SplitBuckets(hash);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void BlockAllocate<Input, Output, Context, FasterSession>(
            int recordSize, 
            out long logicalAddress, 
            FasterExecutionContext<Input, Output, Context> ctx, 
            FasterSession fasterSession)
            where FasterSession : IFasterSession
        {
            while ((logicalAddress = hlog.TryAllocate(recordSize)) == 0)
            {
                InternalRefresh(ctx, fasterSession);
                Thread.Yield();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void BlockAllocateReadCache<Input, Output, Context, FasterSession>(
            int recordSize, 
            out long logicalAddress, 
            FasterExecutionContext<Input, Output, Context> currentCtx, 
            FasterSession fasterSession)
            where FasterSession : IFasterSession
        {
            while ((logicalAddress = readcache.TryAllocate(recordSize)) == 0)
            {
                InternalRefresh(currentCtx, fasterSession);
                Thread.Yield();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TraceBackForKeyMatch(
                                    ref Key key,
                                    long fromLogicalAddress,
                                    long minOffset,
                                    out long foundLogicalAddress,
                                    out long foundPhysicalAddress)
        {
            foundLogicalAddress = fromLogicalAddress;
            while (foundLogicalAddress >= minOffset)
            {
                foundPhysicalAddress = hlog.GetPhysicalAddress(foundLogicalAddress);
                if (comparer.Equals(ref key, ref hlog.GetKey(foundPhysicalAddress)))
                {
                    return true;
                }
                else
                {
                    foundLogicalAddress = hlog.GetInfo(foundPhysicalAddress).PreviousAddress;
                    //This makes testing REALLY slow
                    //Debug.WriteLine("Tracing back");
                    continue;
                }
            }
            foundPhysicalAddress = Constants.kInvalidAddress;
            return false;
        }
        #endregion

        #region Split Index
        private void SplitBuckets(long hash)
        {
            long masked_bucket_index = hash & state[1 - resizeInfo.version].size_mask;
            int offset = (int)(masked_bucket_index >> Constants.kSizeofChunkBits);

            int numChunks = (int)(state[1 - resizeInfo.version].size / Constants.kSizeofChunk);
            if (numChunks == 0) numChunks = 1; // at least one chunk


            if (!Utility.IsPowerOfTwo(numChunks))
            {
                throw new FasterException("Invalid number of chunks: " + numChunks);
            }
            for (int i = offset; i < offset + numChunks; i++)
            {
                if (0 == Interlocked.CompareExchange(ref splitStatus[i & (numChunks - 1)], 1, 0))
                {
                    long chunkSize = state[1 - resizeInfo.version].size / numChunks;
                    long ptr = chunkSize * (i & (numChunks - 1));

                    HashBucket* src_start = state[1 - resizeInfo.version].tableAligned + ptr;
                    HashBucket* dest_start0 = state[resizeInfo.version].tableAligned + ptr;
                    HashBucket* dest_start1 = state[resizeInfo.version].tableAligned + state[1 - resizeInfo.version].size + ptr;

                    SplitChunk(src_start, dest_start0, dest_start1, chunkSize);

                    // split for chunk is done
                    splitStatus[i & (numChunks - 1)] = 2;

                    if (Interlocked.Decrement(ref numPendingChunksToBeSplit) == 0)
                    {
                        // GC old version of hash table
                        state[1 - resizeInfo.version] = default;
                        GlobalStateMachineStep(systemState);
                        return;
                    }
                    break;
                }
            }

            while (Interlocked.Read(ref splitStatus[offset & (numChunks - 1)]) == 1)
            {

            }

        }

        private void SplitChunk(
                    HashBucket* _src_start,
                    HashBucket* _dest_start0,
                    HashBucket* _dest_start1,
                    long chunkSize)
        {
            for (int i = 0; i < chunkSize; i++)
            {
                var src_start = _src_start + i;

                long* left = (long*)(_dest_start0 + i);
                long* right = (long*)(_dest_start1 + i);
                long* left_end = left + Constants.kOverflowBucketIndex;
                long* right_end = right + Constants.kOverflowBucketIndex;

                HashBucketEntry entry = default;
                do
                {
                    for (int index = 0; index < Constants.kOverflowBucketIndex; ++index)
                    {
                        entry.word = *(((long*)src_start) + index);
                        if (Constants.kInvalidEntry == entry.word)
                        {
                            continue;
                        }

                        var logicalAddress = entry.Address;
                        if (logicalAddress >= hlog.HeadAddress)
                        {
                            var physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
                            var hash = comparer.GetHashCode64(ref hlog.GetKey(physicalAddress));
                            if ((hash & state[resizeInfo.version].size_mask) >> (state[resizeInfo.version].size_bits - 1) == 0)
                            {
                                // Insert in left
                                if (left == left_end)
                                {
                                    var new_bucket = (HashBucket*)overflowBucketsAllocator.Allocate();
                                    *left = (long)new_bucket;
                                    left = (long*)new_bucket;
                                    left_end = left + Constants.kOverflowBucketIndex;
                                }

                                *left = entry.word;
                                left++;

                                // Insert previous address in right
                                entry.Address = TraceBackForOtherChainStart(hlog.GetInfo(physicalAddress).PreviousAddress, 1);
                                if (entry.Address != Constants.kInvalidAddress)
                                {
                                    if (right == right_end)
                                    {
                                        var new_bucket = (HashBucket*)overflowBucketsAllocator.Allocate();
                                        *right = (long)new_bucket;
                                        right = (long*)new_bucket;
                                        right_end = right + Constants.kOverflowBucketIndex;
                                    }

                                    *right = entry.word;
                                    right++;
                                }
                            }
                            else
                            {
                                // Insert in right
                                if (right == right_end)
                                {
                                    var new_bucket = (HashBucket*)overflowBucketsAllocator.Allocate();
                                    *right = (long)new_bucket;
                                    right = (long*)new_bucket;
                                    right_end = right + Constants.kOverflowBucketIndex;
                                }

                                *right = entry.word;
                                right++;

                                // Insert previous address in left
                                entry.Address = TraceBackForOtherChainStart(hlog.GetInfo(physicalAddress).PreviousAddress, 0);
                                if (entry.Address != Constants.kInvalidAddress)
                                {
                                    if (left == left_end)
                                    {
                                        var new_bucket = (HashBucket*)overflowBucketsAllocator.Allocate();
                                        *left = (long)new_bucket;
                                        left = (long*)new_bucket;
                                        left_end = left + Constants.kOverflowBucketIndex;
                                    }

                                    *left = entry.word;
                                    left++;
                                }
                            }
                        }
                        else
                        {
                            // Insert in both new locations

                            // Insert in left
                            if (left == left_end)
                            {
                                var new_bucket = (HashBucket*)overflowBucketsAllocator.Allocate();
                                *left = (long)new_bucket;
                                left = (long*)new_bucket;
                                left_end = left + Constants.kOverflowBucketIndex;
                            }

                            *left = entry.word;
                            left++;

                            // Insert in right
                            if (right == right_end)
                            {
                                var new_bucket = (HashBucket*)overflowBucketsAllocator.Allocate();
                                *right = (long)new_bucket;
                                right = (long*)new_bucket;
                                right_end = right + Constants.kOverflowBucketIndex;
                            }

                            *right = entry.word;
                            right++;
                        }
                    }

                    if (*(((long*)src_start) + Constants.kOverflowBucketIndex) == 0) break;
                    src_start = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(*(((long*)src_start) + Constants.kOverflowBucketIndex));
                } while (true);
            }
        }

        private long TraceBackForOtherChainStart(long logicalAddress, int bit)
        {
            while (logicalAddress >= hlog.HeadAddress)
            {
                var physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
                var hash = comparer.GetHashCode64(ref hlog.GetKey(physicalAddress));
                if ((hash & state[resizeInfo.version].size_mask) >> (state[resizeInfo.version].size_bits - 1) == bit)
                {
                    return logicalAddress;
                }
                logicalAddress = hlog.GetInfo(physicalAddress).PreviousAddress;
            }
            return logicalAddress;
        }
        #endregion

        #region Read Cache
        private bool ReadFromCache(ref Key key, ref long logicalAddress, ref long physicalAddress)
        {
            HashBucketEntry entry = default;
            entry.word = logicalAddress;
            if (!entry.ReadCache) return false;

            physicalAddress = readcache.GetPhysicalAddress(logicalAddress & ~Constants.kReadCacheBitMask);

            while (true)
            {
                if (!readcache.GetInfo(physicalAddress).Invalid && comparer.Equals(ref key, ref readcache.GetKey(physicalAddress)))
                {
                    if ((logicalAddress & ~Constants.kReadCacheBitMask) >= readcache.SafeReadOnlyAddress)
                    {
                        return true;
                    }
                    Debug.Assert((logicalAddress & ~Constants.kReadCacheBitMask) >= readcache.SafeHeadAddress);
                    // TODO: copy to tail of read cache
                    // and return new cache entry
                }

                logicalAddress = readcache.GetInfo(physicalAddress).PreviousAddress;
                entry.word = logicalAddress;
                if (!entry.ReadCache) break;
                physicalAddress = readcache.GetPhysicalAddress(logicalAddress & ~Constants.kReadCacheBitMask);
            }
            physicalAddress = 0;
            return false;
        }

        private void SkipReadCache(ref long logicalAddress)
        {
            HashBucketEntry entry = default;
            entry.word = logicalAddress;
            if (!entry.ReadCache) return;

            var physicalAddress = readcache.GetPhysicalAddress(logicalAddress & ~Constants.kReadCacheBitMask);

            while (true)
            {
                logicalAddress = readcache.GetInfo(physicalAddress).PreviousAddress;
                entry.word = logicalAddress;
                if (!entry.ReadCache) return;
                physicalAddress = readcache.GetPhysicalAddress(logicalAddress & ~Constants.kReadCacheBitMask);
            }
        }

        private void SkipAndInvalidateReadCache(ref long logicalAddress, ref Key key)
        {
            HashBucketEntry entry = default;
            entry.word = logicalAddress;
            if (!entry.ReadCache) return;

            var physicalAddress = readcache.GetPhysicalAddress(logicalAddress & ~Constants.kReadCacheBitMask);

            while (true)
            {
                // Invalidate read cache entry if key found
                if (comparer.Equals(ref key, ref readcache.GetKey(physicalAddress)))
                {
                    readcache.GetInfo(physicalAddress).Invalid = true;
                }

                logicalAddress = readcache.GetInfo(physicalAddress).PreviousAddress;
                entry.word = logicalAddress;
                if (!entry.ReadCache) return;
                physicalAddress = readcache.GetPhysicalAddress(logicalAddress & ~Constants.kReadCacheBitMask);
            }
        }

        private void ReadCacheEvict(long fromHeadAddress, long toHeadAddress)
        {
            var bucket = default(HashBucket*);
            var slot = default(int);
            var logicalAddress = Constants.kInvalidAddress;
            var physicalAddress = default(long);

            HashBucketEntry entry = default;
            logicalAddress = fromHeadAddress;

            while (logicalAddress < toHeadAddress)
            {
                physicalAddress = readcache.GetPhysicalAddress(logicalAddress);
                var recordSize = readcache.GetRecordSize(physicalAddress);
                ref RecordInfo info = ref readcache.GetInfo(physicalAddress);
                if (!info.Invalid)
                {
                    ref Key key = ref readcache.GetKey(physicalAddress);
                    entry.word = info.PreviousAddress;
                    if (!entry.ReadCache)
                    {
                        var hash = comparer.GetHashCode64(ref key);
                        var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

                        entry = default;
                        var tagExists = FindTag(hash, tag, ref bucket, ref slot, ref entry);
                        while (tagExists && entry.ReadCache)
                        {
                            var updatedEntry = default(HashBucketEntry);
                            updatedEntry.Tag = tag;
                            updatedEntry.Address = info.PreviousAddress;
                            updatedEntry.Pending = entry.Pending;
                            updatedEntry.Tentative = false;

                            if (entry.word == Interlocked.CompareExchange
                                (ref bucket->bucket_entries[slot], updatedEntry.word, entry.word))
                                break;

                            tagExists = FindTag(hash, tag, ref bucket, ref slot, ref entry);
                        }
                    }
                }
                logicalAddress += recordSize;
                if ((logicalAddress & readcache.PageSizeMask) + recordSize > readcache.PageSize)
                {
                    logicalAddress = (1 + (logicalAddress >> readcache.LogPageSizeBits)) << readcache.LogPageSizeBits;
                    continue;
                }
            }
        }

        private long GetLatestRecordVersion(ref HashBucketEntry entry, long defaultVersion)
        {
            if (UseReadCache && entry.ReadCache)
            {
                var _addr = readcache.GetPhysicalAddress(entry.Address & ~Constants.kReadCacheBitMask);
                if (entry.Address >= readcache.HeadAddress)
                    return readcache.GetInfo(_addr).Version;
                else
                    return defaultVersion;
            }
            else
            {
                var _addr = hlog.GetPhysicalAddress(entry.Address);
                if (entry.Address >= hlog.HeadAddress)
                    return hlog.GetInfo(_addr).Version;
                else
                    return defaultVersion;
            }
        }
        #endregion
    }
}
