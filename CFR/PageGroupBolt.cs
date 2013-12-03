﻿using Microsoft.WindowsAzure;
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
    class PageGroupBolt : IBolt
    {
        private IEmitter emitter;
        private Dictionary<string, int> localCache = new Dictionary<string, int>();
        private CloudTable table;
        private TopologyContext context;

        public PageGroupBolt()
        {
            Microsoft.WindowsAzure.Storage.CloudStorageAccount storageAccount = GetStorageAccount();

            // Create the table client.
            Microsoft.WindowsAzure.Storage.Table.CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            this.table = tableClient.GetTableReference("page");

            this.table.CreateIfNotExists();
        }

        public void Execute(PrimitiveInterface.Tuple tuple)
        {
            string tagId = tuple.Get(0) as string;
            string dateTime = tuple.Get(1) as string;
            string page = tuple.Get(2) as string;
            string location = tuple.Get(3) as string;

            string key = page + "_" + dateTime;

            var parts = dateTime.Split(new char[] { '/' });

            if (localCache.ContainsKey(key))
            {
                localCache[key]++;
            }
            else
            {
                localCache[key] = 1;
            }

            // Not necessary to update database for each entry
            if (localCache[key] % 100 == 0)
            {
                PageTotalCountEntry entity = new PageTotalCountEntry()
                {
                    Name = key,
                    Count = localCache[key],
                    Bolt = this.context.ActorId,
                    RowKey = page + "_" + parts[0] + "_" + parts[1] + "_" + parts[2] + "_" + parts[3],
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
    public class PageTotalCountEntry : TableEntity
    {
        /// <summary>
        /// Suppose Topology Table is pretty small, so it's not necessary to Partition
        /// </summary>
        public const string Key = "Page";

        public PageTotalCountEntry()
        {
            this.PartitionKey = PageTotalCountEntry.Key;
        }

        public string Name { get; set; }

        public int Count { get; set; }

        public string Bolt { get; set; }
    }
}
