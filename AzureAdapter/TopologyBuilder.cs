using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureAdapter
{
    public class TopologyBuilder
    {
        private static SpoutType spoutType = SpoutType.CFR;

        public static void DoAssignment(ActorAssignment assignment)
        {
            CloudTable table = StorageAccount.GetTable("topology");
            TableOperation mergeOperation = TableOperation.InsertOrReplace(assignment);
            TableResult retrievedResult = table.Execute(mergeOperation);
        }

        public static IList<TopologyMetadata> GetTopologyMetadata()
        {
            CloudTable table = StorageAccount.GetTable("metadata");

            // Build word count topology for test
            if (RoleEnvironment.IsEmulated)
            {
                if (spoutType == SpoutType.WordCount)
                {
                    foreach (ActorMetadata entity in wordCountMetadatas)
                    {
                        TableOperation insertOperation = TableOperation.InsertOrReplace(entity);
                        table.Execute(insertOperation);
                    }
                }
                else if (spoutType == SpoutType.CFR)
                {
                    foreach (ActorMetadata entity in cfrMetadatas)
                    {
                        TableOperation insertOperation = TableOperation.InsertOrReplace(entity);
                        table.Execute(insertOperation);
                    }
                }
            }

            // Construct the query operation for all customer entities where PartitionKey="Smith".
            TableQuery<ActorMetadata> query = new TableQuery<ActorMetadata>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, ActorMetadata.Key));

            Dictionary<string, TopologyMetadata> metadata = new Dictionary<string, TopologyMetadata>();

            // Print the fields for each customer.
            foreach (ActorMetadata entity in table.ExecuteQuery(query))
            {
                if (!metadata.Keys.Contains(entity.Topology))
                {
                    metadata[entity.Topology] = new TopologyMetadata() { Name = entity.Topology };
                }

                metadata[entity.Topology].Actors.Add(entity);
            }

            return metadata.Values.ToList();
        }

        public enum SpoutType
        {
            WordCount = 0,
            CFR = 1,
            DataQuality = 2
        }

        #region Test Data
        private static List<ActorMetadata> wordCountMetadatas = new List<ActorMetadata>()
        {
            new ActorMetadata("WordReadSpout", "WordCountTopology")
                    {
                        IsSpout = true,
                        SchemaGroupingMode = "ShuffleGrouping",
                        ParallelCount = 1,
                    },

            new ActorMetadata("WordNormalizeBolt", "WordCountTopology")
                    {
                        IsSpout = false,
                        SchemaGroupingMode = "FieldGrouping",
                        GroupingField = "word",
                        ParallelCount = 2,
                        Parent = "WordReadSpout",
                    },

            new ActorMetadata("WordCountBolt", "WordCountTopology")
                    {
                        IsSpout = false,
                        ParallelCount = 2,
                        Parent = "WordNormalizeBolt",
                    },
        };

        private static List<ActorMetadata> cfrMetadatas = new List<ActorMetadata>()
        {
            new ActorMetadata("TagIdSpout", "CFRTopology")
                    {
                        IsSpout = true,
                        SchemaGroupingMode = "FieldGrouping",
                        GroupingField = "tagId,dateTime",
                        ParallelCount = 1,
                    },

            new ActorMetadata("TagIdGroupBolt", "CFRTopology")
                    {
                        IsSpout = false,
                        SchemaGroupingMode = "FieldGrouping",
                        GroupingField = "page,dateTime",
                        ParallelCount = 2,
                        Parent = "TagIdSpout",
                    },

            new ActorMetadata("PageGroupBolt", "CFRTopology")
                    {
                        IsSpout = false,
                        SchemaGroupingMode = "FieldGrouping",
                        GroupingField = "report,dateTime",
                        ParallelCount = 2,
                        Parent = "TagIdGroupBolt",
                    },

            new ActorMetadata("ReportGroupCompletnessBolt", "CFRTopology")
                    {
                        IsSpout = false,
                        ParallelCount = 2,
                        Parent = "PageGroupBolt",
                    },
        };
        #endregion
    }
}
