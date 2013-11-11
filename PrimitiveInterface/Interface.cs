using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PrimitiveInterface
{
    public interface ISpout
    {
        void NextTuple();
    }

    public interface IBolt
    {
    }
}
