using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PrimitiveInterface
{
    public interface IEmitter
    {
        // $TODO: we need strong typed value system
        void Emit(string value);
    }

    public interface ISpout
    {
        void Open(IEmitter emitter);
        void Execute();
    }

    public interface IBolt
    {
        void Open(IEmitter emitter);
        void Execute(string tuple);
    }
}
