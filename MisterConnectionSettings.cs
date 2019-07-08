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

        public MisterConnectionSettings()
        {
            CheckpointIntervalMilliseconds = 10 * 60 * 1000;
            CheckpointCleanCount = 16;

            WorkerThreadCount = 4;
            WorkerRefreshIntervalMilliseconds = 1000;
        }
    }
}
