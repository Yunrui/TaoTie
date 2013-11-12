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

        public void Execute(PrimitiveInterface.Tuple tuple)
        {
            string value = tuple.Get(0) as string;

            if (string.IsNullOrWhiteSpace(value))
            {
                return;
            }

            if (wordsCount.ContainsKey(value))
            {
                wordsCount[value]++;
            }
            else
            {
                wordsCount[value] = 1;
            }

            Trace.TraceInformation("{0} : {1}", tuple, wordsCount[value]);
        }

        public void Open(IEmitter emitter)
        {
            this.emitter = emitter;
        }
    }
}
