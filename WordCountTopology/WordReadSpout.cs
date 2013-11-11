using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using PrimitiveInterface;

namespace WordCountTopology
{
    /// <summary>
    /// This is an example for TaoTie, make sure copy it manually to approot
    /// </summary>
    [Export(typeof(ISpout))]
    public class WordReadSpout : ISpout
    {
        public void NextTuple()
        {
        }
    }
}
