﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections;
using System.Collections.Generic;

namespace FASTER.core
{
    /// <summary>
    /// Log commit manager
    /// </summary>
    public interface ILogCommitManager : IDisposable
    {
        /// <summary>
        /// Perform (synchronous) commit with specified metadata
        /// </summary>
        /// <param name="beginAddress">Committed begin address (for information only, not necessary to persist)</param>
        /// <param name="untilAddress">Address committed until (for information only, not necessary to persist)</param>
        /// <param name="commitMetadata">Commit metadata - should be persisted</param>
        void Commit(long beginAddress, long untilAddress, byte[] commitMetadata);

        /// <summary>
        /// Return commit metadata
        /// </summary>
        /// <param name="commitNum"></param>
        /// <returns></returns>
        byte[] GetCommitMetadata(long commitNum);

        /// <summary>
        /// Get list of commits, in order of usage preference
        /// </summary>
        /// <returns></returns>
        IEnumerable<long> ListCommits();
    }
}