using System;
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
    public class TagIdSpout : ISpout
    {
        private IEmitter emitter;
        private IList<string> sentences = new List<string>()
        {
            "8819|#|2013/11/18 1:58:59|#|f813b2f7-52cf-449d-8ed9-6962b824e430|#|ab3cba58-4a39-42f6-90e0-069984daece6|#|StaleMailbox1|#|203.45.231.122|#|SEA",
            "8820|#|2013/11/18 1:59:59|#|f813b2f7-52cf-449d-8ed9-6962b824e430|#|ab3cba58-4a39-42f6-90e0-069984daece6|#|StaleMailbox2|#|203.45.231.122|#|SEA",
            "8819|#|2013/11/18 2:50:59|#|f813b2f7-52cf-449d-8ed9-6962b824e430|#|ab3cba58-4a39-42f6-90e0-069984daece6|#|StaleMailbox3|#|203.45.231.122|#|SEA",
            "8819|#|2013/11/18 2:51:59|#|f813b2f7-52cf-449d-8ed9-6962b824e430|#|ab3cba58-4a39-42f6-90e0-069984daece6|#|StaleMailbox4|#|203.45.231.122|#|SEA",
            "8819|#|2013/11/18 2:52:59|#|f813b2f7-52cf-449d-8ed9-6962b824e430|#|ab3cba58-4a39-42f6-90e0-069984daece6|#|StaleMailbox5|#|203.45.231.122|#|SEA",
        };

        public void Execute()
        {
            // The basic pattern is to access data source outside
            // and emit a bunch of tuple then return

            Random random = new Random();

            for (int i = 0; i < 1000; i++)
            {
                string log = this.sentences[random.Next(4)];
                var parts = log.Split(new string[] {"|#|"}, StringSplitOptions.RemoveEmptyEntries);

                var date = DateTime.Parse(parts[1]);
                IList<string> strs = new List<string>()
                    {
                        parts[0],
                        string.Format("{0}/{1}", date.Date.ToShortDateString(), date.Hour),
                        parts[4],
                        parts[5],
                    };

                this.emitter.Emit(new PrimitiveInterface.Tuple(strs));
            }
        }

        public void Open(IEmitter emitter, TopologyContext context)
        {
            this.emitter = emitter;
        }

        public IList<string> DeclareOutputFields()
        {
            return new List<string>() { "tagId", "dateTime", "page", "location" };
        }
    }
}
