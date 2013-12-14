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

namespace DQCompletness
{
    [Export(typeof(IBolt))]
    class ReportGroupCompletnessBolt : IBolt
    {
        private IEmitter emitter;
        private Dictionary<string, int> RequestCountCache = new Dictionary<string, int>();
        private Dictionary<string, double> TotalCompletnessCache = new Dictionary<string, double>(); 

        private CloudTable table;
        private TopologyContext context;

        public ReportGroupCompletnessBolt()
        {
            Microsoft.WindowsAzure.Storage.CloudStorageAccount storageAccount = GetStorageAccount();

            // Create the table client.
            Microsoft.WindowsAzure.Storage.Table.CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            this.table = tableClient.GetTableReference("reportCompletness");

            this.table.CreateIfNotExists();
        }

        public void Execute(PrimitiveInterface.Tuple tuple)
        {
            string dateTime = tuple.Get(0) as string;
            string tenantName = tuple.Get(1) as string;
            string reportName = tuple.Get(2) as string;
            string completness = tuple.Get(3) as string;

            string key = reportName + "_" + dateTime;

            var parts = dateTime.Split(new char[] {'/'});

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

            // Not necessary to update database for each entry
            // Todo update flaged data each 15 seconds, and clear dirty data
            if (RequestCountCache[key] % 50 == 0)
            {
                ReportCompletnessEntry entity = new ReportCompletnessEntry()
                {
                    Name = key,
                    RequestCount = RequestCountCache[key],
                    TotalCompletness = TotalCompletnessCache[key],
                    Completness = TotalCompletnessCache[key] / RequestCountCache[key],
                    Bolt = this.context.ActorId,
                    RowKey = reportName + "_" + parts[0] + "_" + parts[1] + "_" + parts[2] + "_" + parts[3],
                };

                TableOperation insertOperation = TableOperation.InsertOrReplace(entity);
                table.Execute(insertOperation);
            }
        }

        public void Open(IEmitter emitter, TopologyContext context)
        {
            this.emitter = emitter;
            this.context = context;
        }

        public IList<string> DeclareOutputFields()
        {
            return new List<string>() { null };
        }

        private static Microsoft.WindowsAzure.Storage.CloudStorageAccount GetStorageAccount()
        {
            Microsoft.WindowsAzure.Storage.CloudStorageAccount storageAccount = null;

            if (RoleEnvironment.IsEmulated)
            {
                storageAccount = Microsoft.WindowsAzure.Storage.CloudStorageAccount.DevelopmentStorageAccount;
            }
            else
            {
                // Retrieve the storage account from the connection string.
                storageAccount = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("Microsoft.WindowsAzure.Plugins.Diagnostics.ConnectionString"));
            }
            return storageAccount;
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
