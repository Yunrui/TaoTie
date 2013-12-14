using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using PrimitiveInterface;
using System.Diagnostics;

namespace DQCompletness
{

    /// <summary>
    /// This is an example for TaoTie, make sure copy dll manually to approot
    /// </summary>
    [Export(typeof(IBolt))]
    public class DQLogParserBolt : IBolt
    {
        private IEmitter emitter;
  
        public void Execute(PrimitiveInterface.Tuple tuple)
        {
            string delimiter = "|$$|0=";

            string log = tuple.Get(0) as string;
            var parts = log.Split(new string[] { "|#|" }, StringSplitOptions.RemoveEmptyEntries);

            var date = DateTime.Parse(parts[1]);

            int indexDQLog = log.IndexOf(delimiter);

            if (indexDQLog >= 0)
            {
                var DQLog = log.Substring(indexDQLog + delimiter.Length);
                var message = DQLog.Split(new string[] { "|$$|" }, StringSplitOptions.RemoveEmptyEntries)[0];
                DQLogProcessor DQProcessor = new DQLogProcessor(message);
                string reportName = DQProcessor.ReportName;
                string tenantName = DQProcessor.TenantName;
                //if (
                //    reportName.IndexOf("ConnectionbyClientType") >= 0
                // || reportName.IndexOf("MailboxActivity") >= 0
                // || reportName.IndexOf("StaleMailbox") >= 0
                // || reportName.IndexOf("GroupActivity") >= 0
                // || reportName.IndexOf("MailboxUsage") >= 0)
                //{
                    IList<string> strs = new List<string>()
                    {
                        string.Format("{0}/{1}", date.Date.ToShortDateString(), date.Hour),
                        tenantName,
                        reportName,
                        DQProcessor.GetCompletness(date).ToString()
                    };

                    this.emitter.Emit(new PrimitiveInterface.Tuple(strs));
                //}
            }
        }

        public void Open(IEmitter emitter, TopologyContext context)
        {
            this.emitter = emitter;
        }

        public IList<string> DeclareOutputFields()
        {
            return new List<string>() { "dateTime", "tenantName", "report", "completness" };
        }
    }
}
