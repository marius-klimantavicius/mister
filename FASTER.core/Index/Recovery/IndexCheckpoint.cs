﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    public partial class FasterBase
    {
        // Derived class facing persistence API
        internal IndexCheckpointInfo _indexCheckpoint;

        internal void TakeIndexFuzzyCheckpoint()
        {
            var ht_version = resizeInfo.version;

            TakeMainIndexCheckpoint(ht_version,
                                    _indexCheckpoint.main_ht_device,
                                    out ulong ht_num_bytes_written);

            var sectorSize = _indexCheckpoint.main_ht_device.SectorSize;
            var alignedIndexSize = (uint)((ht_num_bytes_written + (sectorSize - 1)) & ~(sectorSize - 1));
            overflowBucketsAllocator.TakeCheckpoint(_indexCheckpoint.main_ht_device, alignedIndexSize, out ulong ofb_num_bytes_written);
            _indexCheckpoint.info.num_ht_bytes = ht_num_bytes_written;
            _indexCheckpoint.info.num_ofb_bytes = ofb_num_bytes_written;
        }

        internal void TakeIndexFuzzyCheckpoint(int ht_version, IDevice device,
                                            out ulong numBytesWritten, IDevice ofbdevice,
                                           out ulong ofbnumBytesWritten, out int num_ofb_buckets)
        {
            TakeMainIndexCheckpoint(ht_version, device, out numBytesWritten);
            var sectorSize = device.SectorSize;
            var alignedIndexSize = (uint)((numBytesWritten + (sectorSize - 1)) & ~(sectorSize - 1));
            overflowBucketsAllocator.TakeCheckpoint(ofbdevice, alignedIndexSize, out ofbnumBytesWritten);
            num_ofb_buckets = overflowBucketsAllocator.GetMaxValidAddress();
        }

        internal bool IsIndexFuzzyCheckpointCompleted()
        {
            bool completed1 = IsMainIndexCheckpointCompleted();
            bool completed2 = overflowBucketsAllocator.IsCheckpointCompleted();
            return completed1 && completed2;
        }

        internal async ValueTask IsIndexFuzzyCheckpointCompletedAsync(CancellationToken token = default)
        {
            // Get tasks first to ensure we have captured the semaphore instances synchronously
            var t1 = IsMainIndexCheckpointCompletedAsync(token);
            var t2 = overflowBucketsAllocator.IsCheckpointCompletedAsync(token);
            await t1;
            await t2;
        }


        // Implementation of an asynchronous checkpointing scheme 
        // for main hash index of FASTER
        private int mainIndexCheckpointCallbackCount;
        private SemaphoreSlim mainIndexCheckpointSemaphore;

        private void TakeMainIndexCheckpoint(int tableVersion,
                                            IDevice device,
                                            out ulong numBytes)
        {
            BeginMainIndexCheckpoint(tableVersion, device, out numBytes);
        }

        private unsafe void BeginMainIndexCheckpoint(
                                           int version,
                                           IDevice device,
                                           out ulong numBytesWritten)
        {
            long totalSize = state[version].size * sizeof(HashBucket);

            int numChunks = 1;
            if (totalSize > uint.MaxValue)
            {
                numChunks = (int)Math.Ceiling((double)totalSize / (long)uint.MaxValue);
                numChunks = (int)Math.Pow(2, Math.Ceiling(Math.Log(numChunks, 2)));
            }

            uint chunkSize = (uint)(totalSize / numChunks);
            mainIndexCheckpointCallbackCount = numChunks;
            mainIndexCheckpointSemaphore = new SemaphoreSlim(0);
            HashBucket* start = state[version].tableAligned;
            
            numBytesWritten = 0;
            for (int index = 0; index < numChunks; index++)
            {
                long chunkStartBucket = (long)start + (index * chunkSize);
                HashIndexPageAsyncFlushResult result = default;
                result.chunkIndex = index;
                device.WriteAsync((IntPtr)chunkStartBucket, numBytesWritten, chunkSize, AsyncPageFlushCallback, result);
                numBytesWritten += chunkSize;
            }
        }

        private bool IsMainIndexCheckpointCompleted()
        {
            return mainIndexCheckpointCallbackCount == 0;
        }

        private async ValueTask IsMainIndexCheckpointCompletedAsync(CancellationToken token = default)
        {
            var s = mainIndexCheckpointSemaphore;
            await s.WaitAsync(token);
            s.Release();
        }

        private unsafe void AsyncPageFlushCallback(uint errorCode, uint numBytes, object context)
        {
            // Set the page status to flushed
            _ = (HashIndexPageAsyncFlushResult)context;

            if (errorCode != 0)
            {
                Trace.TraceError("AsyncPageFlushCallback error: {0}", errorCode);
            }
            if (Interlocked.Decrement(ref mainIndexCheckpointCallbackCount) == 0)
            {
                mainIndexCheckpointSemaphore.Release();
            }
        }
    }
}
