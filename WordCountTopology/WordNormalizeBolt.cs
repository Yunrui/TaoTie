﻿using PrimitiveInterface;
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

        public void Execute(PrimitiveInterface.Tuple tuple)
        {
            string value = tuple.Get(0) as string;

            if (string.IsNullOrWhiteSpace(value))
            {
                return;
            }

            var parts = value.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

            foreach (string word in parts)
            {
                this.emitter.Emit(new PrimitiveInterface.Tuple(word));
            }
        }

        public void Open(IEmitter emitter)
        {
            this.emitter = emitter;
        }

        public IList<string> DeclareOutputFields()
        {
            return new List<string>() { "word" };
        }
    }
}
