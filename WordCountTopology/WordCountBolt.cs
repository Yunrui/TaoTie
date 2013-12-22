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
        private DateTime lastUpdateTime = DateTime.Now;

        private int name = 0;
        public WordCountBolt()
        {
            name = (new Random(DateTime.Now.Millisecond)).Next();

            Microsoft.WindowsAzure.Storage.CloudStorageAccount storageAccount = StorageAccount.GetAccount();

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

            if ((DateTime.Now - this.lastUpdateTime).TotalSeconds > 15)
            {
                this.lastUpdateTime = DateTime.Now;
                foreach (string k in this.wordsCount.Keys)
                {
                    WordCountEntry entity = new WordCountEntry()
                    {
                        Word = k,
                        Count = wordsCount[k],
                        Bolt = this.context.ActorId,
                        RowKey = k + "____" + this.context.ActorId,
                    };

                    TableOperation insertOperation = TableOperation.InsertOrReplace(entity);
                    table.Execute(insertOperation);
                }
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
