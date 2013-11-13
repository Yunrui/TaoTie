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
        void Emit(Tuple value);
    }

    public interface ISpout
    {
        void Open(IEmitter emitter, TopologyContext context);
        void Execute();
        IList<string> DeclareOutputFields();
    }

    public interface IBolt
    {
        void Open(IEmitter emitter, TopologyContext context);
        void Execute(Tuple tuple);
        IList<string> DeclareOutputFields();
    }
}
