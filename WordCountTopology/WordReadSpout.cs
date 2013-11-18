﻿using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using PrimitiveInterface;
using System.Diagnostics;

namespace WordCountTopology
{
    /// <summary>
    /// This is an example for TaoTie, make sure copy dll manually to approot
    /// </summary>
    [Export(typeof(ISpout))]
    public class WordReadSpout : ISpout
    {
        private IEmitter emitter;
        private IList<string> sentences = new List<string>()
        {
            "This is an example of Taotie",
            "It contains one Spout to emit sentences",
            "two Bolts, one is split sentences into word count pair",
            "the other is to do word count",
        };

        public void Execute()
        {
            // The basic pattern is to access data source outside
            // and emit a bunch of tuple then return

            Random random = new Random();

            for (int i = 0; i < 1000; i++)
            {
                this.emitter.Emit(new PrimitiveInterface.Tuple(this.sentences[random.Next(4)]));
            }
        }

        public void Open(IEmitter emitter, TopologyContext context)
        {
            this.emitter = emitter;
        }

        public IList<string> DeclareOutputFields()
        {
            return new List<string>() { "line" };
        }
    }
}
