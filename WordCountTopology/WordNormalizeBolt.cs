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
        private IEmitter emitter;

        public void Execute(string tuple)
        {
            if (string.IsNullOrWhiteSpace(tuple))
            {
                return;
            }

            var parts = tuple.Split(new char[] { ' ' });

            foreach (string word in parts)
            {
                this.emitter.Emit(word);
            }
        }

        public void Open(IEmitter emitter)
        {
            this.emitter = emitter;
        }
    }
}
