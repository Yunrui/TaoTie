using PrimitiveInterface;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WordCountTopology
{
    [Export(typeof(IBolt))]
    class WordCountBolt : IBolt
    {
        private IEmitter emitter;
        private Dictionary<string, int> wordsCount = new Dictionary<string, int>();

        public void Execute(string tuple)
        {
            if (string.IsNullOrWhiteSpace(tuple))
            {
                return;
            }

            if (wordsCount.ContainsKey(tuple))
            {
                wordsCount[tuple]++;
            }
            else
            {
                wordsCount[tuple] = 1;
            }

            Trace.TraceInformation("{0} : {1}", tuple, wordsCount[tuple]);
        }

        public void Open(IEmitter emitter)
        {
            this.emitter = emitter;
        }
    }
}
