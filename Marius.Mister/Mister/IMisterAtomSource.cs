using System;

namespace Marius.Mister
{
    public interface IMisterAtomSource<TAtom> : IDisposable
    {
        ref TAtom GetAtom();
    }
}
