﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.core
{
    internal enum OperationType
    {
        READ,
        RMW,
        UPSERT,
        INSERT,
        DELETE
    }

    internal enum OperationStatus
    {
        SUCCESS,
        NOTFOUND,
        RETRY_NOW,
        RETRY_LATER,
        RECORD_ON_DISK,
        SUCCESS_UNMARK,
        CPR_SHIFT_DETECTED,
        CPR_PENDING_DETECTED
    }

    internal class SerializedFasterExecutionContext
    {
        internal int version;
        internal long serialNum;
        internal string guid;

        /// <summary>
        /// </summary>
        /// <param name="writer"></param>
        public void Write(StreamWriter writer)
        {
            writer.WriteLine(version);
            writer.WriteLine(guid);
            writer.WriteLine(serialNum);
        }

        /// <summary>
        /// </summary>
        /// <param name="reader"></param>
        public void Load(StreamReader reader)
        {
            string value = reader.ReadLine();
            version = int.Parse(value);

            guid = reader.ReadLine();
            value = reader.ReadLine();
            serialNum = long.Parse(value);
        }
    }

    public partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
        where Key : new()
        where Value : new()
    {
        internal struct PendingContext<Input, Output, Context>
        {
            // User provided information
            internal OperationType type;
            internal IHeapContainer<Key> key;
            internal IHeapContainer<Value> value;
            internal Input input;
            internal Output output;
            internal Context userContext;


            // Some additional information about the previous attempt
            internal long id;
            internal int version;
            internal long logicalAddress;
            internal long serialNum;
            internal HashBucketEntry entry;
            internal LatchOperation heldLatch;

            public void Dispose()
            {
                key?.Dispose();
                value?.Dispose();
            }
        }

        internal sealed class FasterExecutionContext<Input, Output, Context> : SerializedFasterExecutionContext
        {
            public Phase phase;
            public bool[] markers;
            public long totalPending;
            public Queue<PendingContext<Input, Output, Context>> retryRequests;
            public Dictionary<long, PendingContext<Input, Output, Context>> ioPendingRequests;
            public AsyncCountDown pendingReads;
            public AsyncQueue<AsyncIOContext<Key, Value>> readyResponses;
            public List<long> excludedSerialNos;
            public int asyncPendingCount;

            public int SyncIoPendingCount => ioPendingRequests.Count - asyncPendingCount;

            public bool HasNoPendingRequests
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get
                {
                    return SyncIoPendingCount == 0 && retryRequests.Count == 0;
                }
            }

            public FasterExecutionContext<Input, Output, Context> prevCtx;
        }
    }

    /// <summary>
    /// Descriptor for a CPR commit point
    /// </summary>
    public struct CommitPoint
    {
        /// <summary>
        /// Serial number until which we have committed
        /// </summary>
        public long UntilSerialNo;

        /// <summary>
        /// List of operation serial nos excluded from commit
        /// </summary>
        public List<long> ExcludedSerialNos;
    }

    /// <summary>
    /// Recovery info for hybrid log
    /// </summary>
    public struct HybridLogRecoveryInfo
    {
        /// <summary>
        /// Guid
        /// </summary>
        public Guid guid;
        /// <summary>
        /// Use snapshot file
        /// </summary>
        public int useSnapshotFile;
        /// <summary>
        /// Version
        /// </summary>
        public int version;
        /// <summary>
        /// Flushed logical address
        /// </summary>
        public long flushedLogicalAddress;
        /// <summary>
        /// Start logical address
        /// </summary>
        public long startLogicalAddress;
        /// <summary>
        /// Final logical address
        /// </summary>
        public long finalLogicalAddress;
        /// <summary>
        /// Head address
        /// </summary>
        public long headAddress;
        /// <summary>
        /// Begin address
        /// </summary>
        public long beginAddress;

        /// <summary>
        /// Commit tokens per session restored during Continue
        /// </summary>
        public ConcurrentDictionary<string, CommitPoint> continueTokens;

        /// <summary>
        /// Commit tokens per session created during Checkpoint
        /// </summary>
        public ConcurrentDictionary<string, CommitPoint> checkpointTokens;

        /// <summary>
        /// Object log segment offsets
        /// </summary>
        public long[] objectLogSegmentOffsets;

        /// <summary>
        /// Initialize
        /// </summary>
        /// <param name="token"></param>
        /// <param name="_version"></param>
        public void Initialize(Guid token, int _version)
        {
            guid = token;
            useSnapshotFile = 0;
            version = _version;
            flushedLogicalAddress = 0;
            startLogicalAddress = 0;
            finalLogicalAddress = 0;
            headAddress = 0;

            continueTokens = new ConcurrentDictionary<string, CommitPoint>();
            checkpointTokens = new ConcurrentDictionary<string, CommitPoint>();

            objectLogSegmentOffsets = null;
        }

        /// <summary>
        /// Initialize from stream
        /// </summary>
        /// <param name="reader"></param>
        public void Initialize(StreamReader reader)
        {
            continueTokens = new ConcurrentDictionary<string, CommitPoint>();

            string value = reader.ReadLine();
            guid = Guid.Parse(value);

            value = reader.ReadLine();
            useSnapshotFile = int.Parse(value);

            value = reader.ReadLine();
            version = int.Parse(value);

            value = reader.ReadLine();
            flushedLogicalAddress = long.Parse(value);

            value = reader.ReadLine();
            startLogicalAddress = long.Parse(value);

            value = reader.ReadLine();
            finalLogicalAddress = long.Parse(value);

            value = reader.ReadLine();
            headAddress = long.Parse(value);

            value = reader.ReadLine();
            beginAddress = long.Parse(value);

            value = reader.ReadLine();
            var numSessions = int.Parse(value);

            for (int i = 0; i < numSessions; i++)
            {
                var guid = reader.ReadLine();
                value = reader.ReadLine();
                var serialno = long.Parse(value);

                var exclusions = new List<long>();
                var exclusionCount = int.Parse(reader.ReadLine());
                for (int j = 0; j < exclusionCount; j++)
                    exclusions.Add(long.Parse(reader.ReadLine()));

                continueTokens.TryAdd(guid, new CommitPoint
                {
                    UntilSerialNo = serialno,
                    ExcludedSerialNos = exclusions
                });
            }

            // Read object log segment offsets
            value = reader.ReadLine();
            var numSegments = int.Parse(value);
            if (numSegments > 0)
            {
                objectLogSegmentOffsets = new long[numSegments];
                for (int i = 0; i < numSegments; i++)
                {
                    value = reader.ReadLine();
                    objectLogSegmentOffsets[i] = long.Parse(value);
                }
            }
        }

        /// <summary>
        ///  Recover info from token
        /// </summary>
        /// <param name="token"></param>
        /// <param name="checkpointManager"></param>
        /// <returns></returns>
        internal void Recover(Guid token, ICheckpointManager checkpointManager)
        {
            var metadata = checkpointManager.GetLogCommitMetadata(token);
            if (metadata == null)
                throw new FasterException("Invalid log commit metadata for ID " + token.ToString());

            using (StreamReader s = new StreamReader(new MemoryStream(metadata)))
                Initialize(s);
        }

        /// <summary>
        /// Write info to byte array
        /// </summary>
        public byte[] ToByteArray()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (StreamWriter writer = new StreamWriter(ms))
                {
                    writer.WriteLine(guid);
                    writer.WriteLine(useSnapshotFile);
                    writer.WriteLine(version);
                    writer.WriteLine(flushedLogicalAddress);
                    writer.WriteLine(startLogicalAddress);
                    writer.WriteLine(finalLogicalAddress);
                    writer.WriteLine(headAddress);
                    writer.WriteLine(beginAddress);

                    writer.WriteLine(checkpointTokens.Count);
                    foreach (var kvp in checkpointTokens)
                    {
                        writer.WriteLine(kvp.Key);
                        writer.WriteLine(kvp.Value.UntilSerialNo);
                        writer.WriteLine(kvp.Value.ExcludedSerialNos.Count);
                        foreach (long item in kvp.Value.ExcludedSerialNos)
                            writer.WriteLine(item);
                    }

                    // Write object log segment offsets
                    writer.WriteLine(objectLogSegmentOffsets == null ? 0 : objectLogSegmentOffsets.Length);
                    if (objectLogSegmentOffsets != null)
                    {
                        for (int i = 0; i < objectLogSegmentOffsets.Length; i++)
                        {
                            writer.WriteLine(objectLogSegmentOffsets[i]);
                        }
                    }
                }
                return ms.ToArray();
            }
        }

        /// <summary>
        /// Print checkpoint info for debugging purposes
        /// </summary>
        public void DebugPrint()
        {
            Debug.WriteLine("******** HybridLog Checkpoint Info for {0} ********", guid);
            Debug.WriteLine("Version: {0}", version);
            Debug.WriteLine("Is Snapshot?: {0}", useSnapshotFile == 1);
            Debug.WriteLine("Flushed LogicalAddress: {0}", flushedLogicalAddress);
            Debug.WriteLine("Start Logical Address: {0}", startLogicalAddress);
            Debug.WriteLine("Final Logical Address: {0}", finalLogicalAddress);
            Debug.WriteLine("Head Address: {0}", headAddress);
            Debug.WriteLine("Begin Address: {0}", beginAddress);
            Debug.WriteLine("Num sessions recovered: {0}", continueTokens.Count);
            Debug.WriteLine("Recovered sessions: ");
            foreach (var sessionInfo in continueTokens.Take(10))
            {
                Debug.WriteLine("{0}: {1}", sessionInfo.Key, sessionInfo.Value);
            }

            if (continueTokens.Count > 10)
                Debug.WriteLine("... {0} skipped", continueTokens.Count - 10);
        }
    }

    internal struct HybridLogCheckpointInfo
    {
        public HybridLogRecoveryInfo info;
        public IDevice snapshotFileDevice;
        public IDevice snapshotFileObjectLogDevice;
        public SemaphoreSlim flushedSemaphore;

        public void Initialize(Guid token, int _version, ICheckpointManager checkpointManager)
        {
            info.Initialize(token, _version);
            checkpointManager.InitializeLogCheckpoint(token);
        }

        public void Recover(Guid token, ICheckpointManager checkpointManager)
        {
            info.Recover(token, checkpointManager);
        }

        public void Reset()
        {
            flushedSemaphore = null;
            info = default;
            if (snapshotFileDevice != null) snapshotFileDevice.Close();
            if (snapshotFileObjectLogDevice != null) snapshotFileObjectLogDevice.Close();
        }

        public bool IsDefault()
        {
            return info.guid == default;
        }
    }

    internal struct IndexRecoveryInfo
    {
        public Guid token;
        public long table_size;
        public ulong num_ht_bytes;
        public ulong num_ofb_bytes;
        public int num_buckets;
        public long startLogicalAddress;
        public long finalLogicalAddress;

        public void Initialize(Guid token, long _size)
        {
            this.token = token;
            table_size = _size;
            num_ht_bytes = 0;
            num_ofb_bytes = 0;
            startLogicalAddress = 0;
            finalLogicalAddress = 0;
            num_buckets = 0;
        }

        public void Initialize(StreamReader reader)
        {
            string value = reader.ReadLine();
            token = Guid.Parse(value);

            value = reader.ReadLine();
            table_size = long.Parse(value);

            value = reader.ReadLine();
            num_ht_bytes = ulong.Parse(value);

            value = reader.ReadLine();
            num_ofb_bytes = ulong.Parse(value);

            value = reader.ReadLine();
            num_buckets = int.Parse(value);

            value = reader.ReadLine();
            startLogicalAddress = long.Parse(value);

            value = reader.ReadLine();
            finalLogicalAddress = long.Parse(value);
        }

        public void Recover(Guid guid, ICheckpointManager checkpointManager)
        {
            var metadata = checkpointManager.GetIndexCommitMetadata(guid);
            if (metadata == null)
                throw new FasterException("Invalid index commit metadata for ID " + guid.ToString());
            using (StreamReader s = new StreamReader(new MemoryStream(metadata)))
                Initialize(s);
        }

        public byte[] ToByteArray()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (var writer = new StreamWriter(ms))
                {

                    writer.WriteLine(token);
                    writer.WriteLine(table_size);
                    writer.WriteLine(num_ht_bytes);
                    writer.WriteLine(num_ofb_bytes);
                    writer.WriteLine(num_buckets);
                    writer.WriteLine(startLogicalAddress);
                    writer.WriteLine(finalLogicalAddress);
                }
                return ms.ToArray();
            }
        }

        public void DebugPrint()
        {
            Debug.WriteLine("******** Index Checkpoint Info for {0} ********", token);
            Debug.WriteLine("Table Size: {0}", table_size);
            Debug.WriteLine("Main Table Size (in GB): {0}", ((double)num_ht_bytes) / 1000.0 / 1000.0 / 1000.0);
            Debug.WriteLine("Overflow Table Size (in GB): {0}", ((double)num_ofb_bytes) / 1000.0 / 1000.0 / 1000.0);
            Debug.WriteLine("Num Buckets: {0}", num_buckets);
            Debug.WriteLine("Start Logical Address: {0}", startLogicalAddress);
            Debug.WriteLine("Final Logical Address: {0}", finalLogicalAddress);
        }

        public void Reset()
        {
            token = default;
            table_size = 0;
            num_ht_bytes = 0;
            num_ofb_bytes = 0;
            num_buckets = 0;
            startLogicalAddress = 0;
            finalLogicalAddress = 0;
        }
    }

    internal struct IndexCheckpointInfo
    {
        public IndexRecoveryInfo info;
        public IDevice main_ht_device;

        public void Initialize(Guid token, long _size, ICheckpointManager checkpointManager)
        {
            info.Initialize(token, _size);
            checkpointManager.InitializeIndexCheckpoint(token);
            main_ht_device = checkpointManager.GetIndexDevice(token);
        }

        public void Recover(Guid token, ICheckpointManager checkpointManager)
        {
            info.Recover(token, checkpointManager);
        }

        public void Reset()
        {
            info = default;
            main_ht_device.Close();
        }

        public bool IsDefault()
        {
            return info.token == default;
        }
    }
}
