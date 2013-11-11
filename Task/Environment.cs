using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;

namespace Task
{
    /// <summary>
    /// Encapsulate Testing and Cloud Environment Config
    /// </summary>
    class Environment
    {
        public static CloudTable GetTopologyTable()
        {
            Microsoft.WindowsAzure.Storage.CloudStorageAccount storageAccount = GetStorageAccount();

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Create the CloudTable object that represents the "people" table.
            CloudTable table = tableClient.GetTableReference("topology");

            table.CreateIfNotExists();

            return table;
        }

        public static void PrepareTestData(Actor actor)
        {
            // $TEST: This code for testing environment only
            if (RoleEnvironment.IsEmulated)
            {
                CloudTable table = Environment.GetTopologyTable();

                string inqueueId = Guid.NewGuid().ToString();
                string outqueueId = Guid.NewGuid().ToString();

                ActorAssignment entity = new ActorAssignment(actor.Id)
                {
                    Topology = "TestTopology",
                    Name = "WordReadSpout",
                    IsSpout = true,
                    InQueue = inqueueId,
                    OutQueue = outqueueId,
                };

                TableOperation insertOperation = TableOperation.InsertOrReplace(entity);
                table.Execute(insertOperation);
            }
        }

        public static CloudQueue GetQueue(string name)
        {
            Microsoft.WindowsAzure.Storage.CloudStorageAccount storageAccount = GetStorageAccount();

            CloudQueueClient client = storageAccount.CreateCloudQueueClient();

            CloudQueue queue = client.GetQueueReference(name);

            queue.CreateIfNotExists();

            return queue;
        }

        /// <summary>
        /// $TODO: consider caching later
        /// </summary>
        /// <returns></returns>
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
}
