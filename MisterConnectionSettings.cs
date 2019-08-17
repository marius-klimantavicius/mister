using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FASTER.core;

namespace Marius.Mister
{
    public class MisterConnectionSettings
    {
        public int CheckpointIntervalMilliseconds { get; set; }
        public int CheckpointCleanCount { get; set; }

        public int WorkerThreadCount { get; set; }
        public int WorkerRefreshIntervalMilliseconds { get; set; }

        public long IndexSize { get; set; }


        /// <summary>
        /// Size of a segment (group of pages), in bits
        /// </summary>
        public int? PageSizeBits;

        /// <summary>
        /// Size of a segment (group of pages), in bits
        /// </summary>
        public int? SegmentSizeBits;

        /// <summary>
        /// Total size of in-memory part of log, in bits
        /// </summary>
        public int? MemorySizeBits;

        /// <summary>
        /// Fraction of log marked as mutable (in-place updates)
        /// </summary>
        public double? MutableFraction;

        public MisterConnectionSettings()
        {
            IndexSize = 1L << 20;

            CheckpointIntervalMilliseconds = 10 * 60 * 1000;
            CheckpointCleanCount = 16;

            WorkerThreadCount = Environment.ProcessorCount;
            WorkerRefreshIntervalMilliseconds = 1000;
        }

        public void Apply(LogSettings logSettings)
        {
            if (PageSizeBits != null) logSettings.PageSizeBits = PageSizeBits.Value;
            if (SegmentSizeBits != null) logSettings.SegmentSizeBits = SegmentSizeBits.Value;
            if (MemorySizeBits != null) logSettings.MemorySizeBits = MemorySizeBits.Value;
            if (MutableFraction != null) logSettings.MutableFraction = MutableFraction.Value;
        }
    }
}
