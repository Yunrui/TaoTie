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

namespace CFR
{
    [Export(typeof(IBolt))]
    class TagIdGroupBolt : IBolt
    {
        private IEmitter emitter;
        private Dictionary<string, int> localCache = new Dictionary<string, int>();
        private CloudTable table;
        private TopologyContext context;
        private DateTime lastUpdateTime = DateTime.Now;

        public TagIdGroupBolt()
        {
            Microsoft.WindowsAzure.Storage.CloudStorageAccount storageAccount = GetStorageAccount();

            // Create the table client.
            Microsoft.WindowsAzure.Storage.Table.CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            this.table = tableClient.GetTableReference("tagId");

            this.table.CreateIfNotExists();
        }

        public void Execute(PrimitiveInterface.Tuple tuple)
        {
            string tagId = tuple.Get(0) as string;
            string dateTime = tuple.Get(1) as string;
            string page = tuple.Get(2) as string;
            string location = tuple.Get(3) as string;
            string tenantName = tuple.Get(4) as string;
            string reportName = tuple.Get(5) as string;
            string completness = tuple.Get(6) as string;

            var parts = dateTime.Split(new char[] {'/'});
            string key = tagId + "_" + parts[0] + "_" + parts[1] + "_" + parts[2] + "_" + parts[3];
            
            if (localCache.ContainsKey(key))
            {
                localCache[key]++;
            }
            else
            {
                localCache[key] = 1;
            }

            if ((DateTime.Now - this.lastUpdateTime).TotalSeconds > 3)
            {
                this.lastUpdateTime = DateTime.Now;

                foreach (string k in this.localCache.Keys)
                {
                    TagIdTotalCountEntry entity = new TagIdTotalCountEntry()
                    {
                        Name = k,
                        Count = localCache[k],
                        Bolt = this.context.ActorId,
                        RowKey = k,
                    };

                    TableOperation insertOperation = TableOperation.InsertOrReplace(entity);
                    table.Execute(insertOperation);
                }
            }

            this.emitter.Emit(tuple);
        }

        public void Open(IEmitter emitter, TopologyContext context)
        {
            this.emitter = emitter;
            this.context = context;
        }

        public IList<string> DeclareOutputFields()
        {
            return new List<string>() { "tagId", "dateTime", "page", "location", "tenantName", "report", "completness" };
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
    public class TagIdTotalCountEntry : TableEntity
    {
        /// <summary>
        /// Suppose Topology Table is pretty small, so it's not necessary to Partition
        /// </summary>
        public const string Key = "TagId";

        public TagIdTotalCountEntry()
        {
            this.PartitionKey = TagIdTotalCountEntry.Key;
        }

        public string Name { get; set; }

        public int Count { get; set; }

        public string Bolt { get; set; }
    }
}
