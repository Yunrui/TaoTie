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
        public static CloudTable GetTable(string name)
        {
            Microsoft.WindowsAzure.Storage.CloudStorageAccount storageAccount = GetStorageAccount();

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            CloudTable table = tableClient.GetTableReference(name);

            table.CreateIfNotExists();

            return table;
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

        #region Prepare Test Data

        private static int ActorStep = 0;

        private static List<ActorAssignment> assignments = new List<ActorAssignment>()
        {
            new ActorAssignment(Guid.Empty)
                    {
                        Topology = "TestTopology",
                        Name = "WordReadSpout",
                        IsSpout = true,
                        InQueue = string.Empty,
                        OutQueues = "spoutoutput1,spoutoutput2",
                        SchemaGroupingMode = "ShuffleGrouping",
                    },

            new ActorAssignment(Guid.Empty)
                    {
                        Topology = "TestTopology",
                        Name = "WordNormalizeBolt",
                        IsSpout = false,
                        InQueue = "spoutoutput1",
                        OutQueues = "wnboltoutput1,wnboltoutput2",
                        SchemaGroupingMode = "FieldGrouping",
                        GroupingField = "word",
                    },

            new ActorAssignment(Guid.Empty)
                    {
                        Topology = "TestTopology",
                        Name = "WordNormalizeBolt",
                        IsSpout = false,
                        InQueue = "spoutoutput2",
                        OutQueues = "wnboltoutput1,wnboltoutput2",
                        SchemaGroupingMode = "FieldGrouping",
                        GroupingField = "word",
                    },

            new ActorAssignment(Guid.Empty)
                    {
                        Topology = "TestTopology",
                        Name = "WordCountBolt",
                        IsSpout = false,
                        InQueue = "wnboltoutput1",
                        OutQueues = null,
                    },

            new ActorAssignment(Guid.Empty)
                    {
                        Topology = "TestTopology",
                        Name = "WordCountBolt",
                        IsSpout = false,
                        InQueue = "wnboltoutput2",
                        OutQueues = null,
                    },
        };

        public static void PrepareTestData(Actor actor)
        {
            // $TEST: This code for testing environment only
            if (RoleEnvironment.IsEmulated)
            {
                CloudTable table = Environment.GetTable("topology");
                ActorAssignment entity = assignments[ActorStep++];

                entity.RowKey = actor.Id.ToString();

                TableOperation insertOperation = TableOperation.InsertOrReplace(entity);
                table.Execute(insertOperation);
            }
        }
        #endregion
    }
}
