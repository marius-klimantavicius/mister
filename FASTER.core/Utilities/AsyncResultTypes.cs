﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#define CALLOC

using System;
using System.Threading;

namespace FASTER.core
{
    internal struct AsyncGetFromDiskResult<TContext> : IAsyncResult
    {
        public TContext context;

        public bool IsCompleted => throw new NotImplementedException();

        public WaitHandle AsyncWaitHandle => throw new NotImplementedException();

        public object AsyncState => throw new NotImplementedException();

        public bool CompletedSynchronously => throw new NotImplementedException();
    }

    internal unsafe struct HashIndexPageAsyncFlushResult : IAsyncResult
    {
        public int chunkIndex;

        public bool IsCompleted => throw new NotImplementedException();

		public WaitHandle AsyncWaitHandle => throw new NotImplementedException();

		public object AsyncState => throw new NotImplementedException();

		public bool CompletedSynchronously => throw new NotImplementedException();
	}

    internal unsafe struct HashIndexPageAsyncReadResult : IAsyncResult
    {
        public int chunkIndex;

        public bool IsCompleted => throw new NotImplementedException();

        public WaitHandle AsyncWaitHandle => throw new NotImplementedException();

        public object AsyncState => throw new NotImplementedException();

        public bool CompletedSynchronously => throw new NotImplementedException();
    }

    internal struct OverflowPagesFlushAsyncResult : IAsyncResult
    {
        public bool IsCompleted => throw new NotImplementedException();

        public WaitHandle AsyncWaitHandle => throw new NotImplementedException();

        public object AsyncState => throw new NotImplementedException();

        public bool CompletedSynchronously => throw new NotImplementedException();
    }

    internal struct OverflowPagesReadAsyncResult : IAsyncResult
    {

        public bool IsCompleted => throw new NotImplementedException();

        public WaitHandle AsyncWaitHandle => throw new NotImplementedException();

        public object AsyncState => throw new NotImplementedException();

        public bool CompletedSynchronously => throw new NotImplementedException();
    }
}
