using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Marius.Mister
{
    public class MisterConnectionSettings
    {
        public int CheckpointIntervalMilliseconds { get; set; }
        public int CheckpointCleanCount { get; set; }

        public int WorkerThreadCount { get; set; }
        public int WorkerRefreshIntervalMilliseconds { get; set; }

        public long IndexSize { get; set; }

        public MisterConnectionSettings()
        {
            IndexSize = 1L << 20;

            CheckpointIntervalMilliseconds = 10 * 60 * 1000;
            CheckpointCleanCount = 16;

            WorkerThreadCount = Environment.ProcessorCount;
            WorkerRefreshIntervalMilliseconds = 1000;
        }
    }
}
