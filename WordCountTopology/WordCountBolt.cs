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

namespace WordCountTopology
{
    [Export(typeof(IBolt))]
    class WordCountBolt : IBolt
    {
        private IEmitter emitter;
        private Dictionary<string, int> wordsCount = new Dictionary<string, int>();
        private CloudTable table;
        private TopologyContext context;

        private int name = 0;
        public WordCountBolt()
        {
            name = (new Random(DateTime.Now.Millisecond)).Next();

            Microsoft.WindowsAzure.Storage.CloudStorageAccount storageAccount = GetStorageAccount();

            // Create the table client.
            Microsoft.WindowsAzure.Storage.Table.CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            this.table = tableClient.GetTableReference("wordcount");

            this.table.CreateIfNotExists();
        }

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

            // Not necessary to update database for each entry
            if (wordsCount[value] % 1000 == 0)
            {
                WordCountEntry entity = new WordCountEntry()
                {
                    Word = value,
                    Count = wordsCount[value],
                    Bolt = this.context.ActorId,
                    RowKey = value + "____" + this.context.ActorId,
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
            return null;
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
    public class WordCountEntry : TableEntity
    {
        /// <summary>
        /// Suppose Topology Table is pretty small, so it's not necessary to Partition
        /// </summary>
        public const string Key = "WordCount";

        public WordCountEntry()
        {
            this.PartitionKey = WordCountEntry.Key;
        }

        public string Word { get; set; }

        public int Count { get; set; }

        public string Bolt { get; set; }
    }
}
