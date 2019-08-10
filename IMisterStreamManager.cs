using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Marius.Mister
{
    public interface IMisterStreamManager
    {
        MemoryStream GetStream();
    }
}
