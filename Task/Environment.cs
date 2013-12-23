using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;
using AzureAdapter;

namespace Task
{
    /// <summary>
    /// Encapsulate Testing and Cloud Environment Config
    /// </summary>
    class Environment
    {
        #region Prepare Test Data

        private static int ActorStep = 0;
        private static int Example = 0;

        private static List<ActorAssignment> DQCompletnessAssignments = new List<ActorAssignment>()
        {
            new ActorAssignment(Guid.Empty)
                    {
                        Topology = "DQTopology",
                        Name = "DQLogSpout",
                        IsSpout = true,
                        InQueue = string.Empty,
                        OutQueues = "dqspoutoutput1,dqspoutoutput2",
                        SchemaGroupingMode = "ShuffleGrouping",
                        HeartBeat = DateTime.UtcNow,
                    },

            new ActorAssignment(Guid.Empty)
                    {
                        Topology = "DQTopology",
                        Name = "DQLogParserBolt",
                        IsSpout = false,
                        InQueue = "dqspoutoutput1",
                        OutQueues = "dqparserboltoutput1,dqparserboltoutput2",
                        SchemaGroupingMode = "FieldGrouping",
                        GroupingField = "dateTime,report",
                        HeartBeat = DateTime.UtcNow,
                    },

            new ActorAssignment(Guid.Empty)
                    {
                        Topology = "DQTopology",
                        Name = "DQLogParserBolt",
                        IsSpout = false,
                        InQueue = "dqspoutoutput2",
                        OutQueues = "dqparserboltoutput1,dqparserboltoutput2",
                        SchemaGroupingMode = "FieldGrouping",
                        GroupingField = "dateTime,report",
                        HeartBeat = DateTime.UtcNow,
                    },

            new ActorAssignment(Guid.Empty)
                    {
                        Topology = "DQTopology",
                        Name = "ReportGroupCompletnessBolt",
                        IsSpout = false,
                        InQueue = "dqparserboltoutput1",
                        OutQueues = null,
                        HeartBeat = DateTime.UtcNow,
                    },

            new ActorAssignment(Guid.Empty)
                    {
                        Topology = "DQTopology",
                        Name = "ReportGroupCompletnessBolt",
                        IsSpout = false,
                        InQueue = "dqparserboltoutput2",
                        OutQueues = null,
                        HeartBeat = DateTime.UtcNow,
                    },
        };

        private static List<ActorAssignment> cfrAssignments = new List<ActorAssignment>()
        {
            new ActorAssignment(Guid.Empty)
                    {
                        Topology = "CFRTopology",
                        Name = "TagIdSpout",
                        IsSpout = true,
                        InQueue = string.Empty,
                        OutQueues = "cfroutput1,cfroutput2",
                        SchemaGroupingMode = "FieldGrouping",
                        GroupingField = "tagId,dateTime",
                        HeartBeat = DateTime.UtcNow,
                    },

            new ActorAssignment(Guid.Empty)
                    {
                        Topology = "CFRTopology",
                        Name = "TagIdGroupBolt",
                        IsSpout = false,
                        InQueue = "cfroutput1",
                        OutQueues = "cfroutput3,cfroutput4",
                        SchemaGroupingMode = "FieldGrouping",
                        GroupingField = "page,dateTime",
                        HeartBeat = DateTime.UtcNow,
                    },

            new ActorAssignment(Guid.Empty)
                    {
                        Topology = "CFRTopology",
                        Name = "TagIdGroupBolt",
                        IsSpout = false,
                        InQueue = "cfroutput2",
                        OutQueues = "cfroutput3,cfroutput4",
                        SchemaGroupingMode = "FieldGrouping",
                        GroupingField = "page,dateTime",
                        HeartBeat = DateTime.UtcNow,
                    },

            new ActorAssignment(Guid.Empty)
                    {
                        Topology = "CFRTopology",
                        Name = "PageGroupBolt",
                        IsSpout = false,
                        InQueue = "cfroutput3",
                        HeartBeat = DateTime.UtcNow,
                    },

            new ActorAssignment(Guid.Empty)
                    {
                        Topology = "CFRTopology",
                        Name = "PageGroupBolt",
                        IsSpout = false,
                        InQueue = "cfroutput4",
                        HeartBeat = DateTime.UtcNow,
                    },
        };

        public static void PrepareTestData(Actor actor)
        {
            // $NOTE: master role is taking over this testing method
            if (Example == 0)
            {
                return;
            }

            // $TEST: This code for testing environment only
            if (RoleEnvironment.IsEmulated && ActorStep < 5)
            {
                CloudTable table = StorageAccount.GetTable("topology");
                ActorAssignment entity = null;
                switch(Example)
                {
                    case 1:
                        entity = cfrAssignments[ActorStep++];
                        break;
                    case 2:
                        entity = DQCompletnessAssignments[ActorStep++];
                        break;
                    default:
                        return;
                }

                entity.RowKey = actor.Id.ToString();

                TableOperation insertOperation = TableOperation.InsertOrReplace(entity);
                table.Execute(insertOperation);
            }
        }
        #endregion
    }
}
