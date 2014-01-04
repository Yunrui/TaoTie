using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage.Table;
using PrimitiveInterface;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Queue;

namespace CFR
{
    [Export(typeof(IBolt))]
    class ReportGroupCompletnessBolt : IBolt
    {
        private IEmitter emitter;
        private Dictionary<string, int> RequestCountCache = new Dictionary<string, int>();
        private Dictionary<string, double> TotalCompletnessCache = new Dictionary<string, double>(); 

        private CloudTable table;
        private CloudQueue scrollQueue;
        private TopologyContext context;

        private DateTime lastUpdateTime = DateTime.Now;

        private Queue<PrimitiveInterface.Tuple> latestMessage = new Queue<PrimitiveInterface.Tuple>();

        public ReportGroupCompletnessBolt()
        {
            this.table = StorageAccount.GetTable("reportCompletness");
            this.scrollQueue = StorageAccount.GetQueue("latestaccess");
        }

        public void Execute(PrimitiveInterface.Tuple tuple)
        {
            this.latestMessage.Enqueue(tuple);

            string dateTime = tuple.Get(0) as string;
            string tenantName = tuple.Get(1) as string;
            string reportName = tuple.Get(2) as string;
            string completness = tuple.Get(3) as string;
            string location = tuple.Get(4) as string;

            var parts = dateTime.Split(new char[] { '/' });
            string key = reportName + "_" + parts[0] + "_" + parts[1] + "_" + parts[2] + "_" + parts[3];

            if (RequestCountCache.ContainsKey(key))
            {
                RequestCountCache[key]++;
                TotalCompletnessCache[key] += double.Parse(completness);
            }
            else
            {
                RequestCountCache[key] = 1;
                TotalCompletnessCache[key] = double.Parse(completness);
            }

            if ((DateTime.Now - this.lastUpdateTime).TotalSeconds > 3)
            {
                this.lastUpdateTime = DateTime.Now;

                // Update completeness table
                foreach (string k in this.RequestCountCache.Keys)
                {
                    ReportCompletnessEntry entity = new ReportCompletnessEntry()
                    {
                        Name = k,
                        RequestCount = RequestCountCache[k],
                        TotalCompletness = TotalCompletnessCache[k],
                        Completness = TotalCompletnessCache[k] / RequestCountCache[k],
                        Bolt = this.context.ActorId,
                        RowKey = k,
                    };

                    TableOperation insertOperation = TableOperation.InsertOrReplace(entity);
                    table.Execute(insertOperation);
                }

                // Update latest access queue
                int i = 0; 
                do
                {
                    if (this.latestMessage.Count == 0 || i++ > 10)
                    {
                        break;
                    }

                    var t = this.latestMessage.Dequeue();
                    var str = string.Format("{0}___{1}", t.Get(1), t.Get(2));
                    this.scrollQueue.AddMessage(new CloudQueueMessage(str));

                } while (true);
            }

            var partsdate = dateTime.Split(new char[] { '/' });
            dateTime = partsdate[0] + "/" + partsdate[1] + "/" + partsdate[2];

            IList<string> strs = new List<string>()
                {
                   dateTime,
                   location
                };
            this.emitter.Emit(new PrimitiveInterface.Tuple(strs));
        }

        public void Open(IEmitter emitter, TopologyContext context)
        {
            this.emitter = emitter;
            this.context = context;
        }

        public IList<string> DeclareOutputFields()
        {
            return new List<string>() { "dateTime", "location" };
        }
    }

    /// <summary>
    /// ActorEntity
    /// </summary>
    public class ReportCompletnessEntry : TableEntity
    {
        /// <summary>
        /// Suppose Topology Table is pretty small, so it's not necessary to Partition
        /// </summary>
        public const string Key = "ReportName";

        public ReportCompletnessEntry()
        {
            this.PartitionKey = ReportCompletnessEntry.Key;
        }

        public string Name { get; set; }

        public int RequestCount { get; set; }

        public double TotalCompletness { get; set; }

        public double Completness { get; set; }

        public string Bolt { get; set; }
    }
}
