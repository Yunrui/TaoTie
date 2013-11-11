using PrimitiveInterface;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WordCountTopology
{
    /// <summary>
    /// This is an example for TaoTie, make sure copy dll manually to approot
    /// </summary>
    [Export(typeof(IBolt))]
    class WordNormalizeBolt : IBolt
    {
        public void Execute(string tuple)
        {
        }
    }
}
