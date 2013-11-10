using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Task
{
    class Actor
    {
        public ActorState State
        {
            get;
            set;
        }

        public DateTime HeartBeat
        {
            get;
            set;
        }

        public string DeploymentId
        {
            get;
            set;
        }

        public Guid Id
        {
            get;
            set;
        }
    }

    /// <summary>
    /// The jobs of Service are:
    /// - Send heartbeat to central, indicates "Topology rebuild" if any Task in Error State 
    /// - Born and maintain Task number
    /// </summary>
    class Service
    {
        /// <summary>
        /// We need a thread-safe Dictionary here
        /// </summary>
        private Dictionary<Guid, Actor> actors = new Dictionary<Guid, Actor>();

        /// <summary>
        /// How many Tasks (Spout/Bolt) in a Service Role
        /// </summary>
        private const int ConcurrentTask = 1;

        /// <summary>
        /// ctor
        /// </summary>
        internal Service()
        {
            string deploymentId = RoleEnvironment.DeploymentId;

            // Born #ConcurrentTask threads for Tasks
            // and set state to NewBorn
            for (int i = 0; i < Service.ConcurrentTask; i++)
            {
                // Each actors has its own Guid Identifier
                Guid id = Guid.NewGuid();
                Actor actor = new Actor() { State = ActorState.NewBorn, Id = id, DeploymentId = deploymentId, HeartBeat = DateTime.UtcNow };

                ThreadPool.QueueUserWorkItem(new WaitCallback(ThreadProc), actor);

                this.actors[id] = actor;
            }
        }

        /// <summary>
        /// - Send Heartbeat to Central
        ///     - Service Up/Down
        ///     - Actor State
        /// - Kill Error Actor and Keep Actor Number;
        /// </summary>
        internal void Run()
        {
            // $TODO: Lots of ToDo here
            Trace.TraceInformation("Service Maintain {0}", RoleEnvironment.DeploymentId);
        }

        // The thread procedure performs the independent task
        static void ThreadProc(Object stateInfo)
        {
            Actor actor = (Actor)stateInfo;

            // $TODO: easy instrumentation framework
            Trace.TraceInformation("Deployment {0} Actor {1} is Initialized.", actor.DeploymentId, actor.Id);
            
            while (true)
            {
                SceneLogger.Current.SetSceneId(actor);
                
                if (actor.State == ActorState.NewBorn)
                {
                    // Needs to contact central to get new assignment

                    // if there is an assignment then
                    if (GetAssignment(actor) != null)
                    {
                        actor.State = ActorState.Working;
                        actor.HeartBeat = DateTime.UtcNow;

                        SceneLogger.Current.Log("Get Assignment and switch State to Working.");
                    }
                    else
                    {
                        actor.HeartBeat = DateTime.UtcNow;

                        SceneLogger.Current.Log("Waiting for assignment.");

                        // Let's check assignment 10 seconds later
                        Thread.Sleep(10000);
                    } 
                }
                else if (actor.State == ActorState.Working)
                {
                    SceneLogger.Current.Log("Working on the assignment");

                    // Let's check assignment 10 seconds later
                    Thread.Sleep(10000);
                }
                else if (actor.State == ActorState.Error)
                {
                    SceneLogger.Current.Log("The Actor is shutdown due to Error State.");
                    break;
                }
            }
        }

        static ActorEntity GetAssignment(Actor actor)
        {
            ActorEntity actorEntity = null;

            try
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

                // Create the table client.
                CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

                // Create the CloudTable object that represents the "people" table.
                CloudTable table = tableClient.GetTableReference("topology");

                // $TEST: This code for testing environment only
                // $TODO: consider to move it to a separate place
                if (RoleEnvironment.IsEmulated)
                {
                    table.CreateIfNotExists();

                    ActorEntity entity = new ActorEntity(actor.Id)
                    {
                        Topology = "TestTopology"
                    };

                    TableOperation insertOperation = TableOperation.InsertOrReplace(entity);
                    table.Execute(insertOperation);
                }

                // Create a retrieve operation that takes a customer entity.
                TableOperation retrieveOperation = TableOperation.Retrieve<ActorEntity>(ActorEntity.Key, actor.Id.ToString());

                // Execute the retrieve operation.
                TableResult retrievedResult = table.Execute(retrieveOperation);

                // Get Assignment
                if (retrievedResult.Result != null)
                {
                    actorEntity = (ActorEntity)retrievedResult.Result;
                    SceneLogger.Current.Log(string.Format("Get {0} Assignment from topology {1}, ", actorEntity.IsSpout ? "Spout" : "Bolt", actorEntity.Topology));
                }
            }
            catch (Exception e)
            {
                SceneLogger.Current.Log(e.Message);
            }

            return actorEntity;
        }
    }

    /// <summary>
    /// ActorEntity
    /// </summary>
    public class ActorEntity : TableEntity
    {
        /// <summary>
        /// Suppose Topology Table is pretty small, so it's not necessary to Partition
        /// </summary>
        public const string Key = "Actor";

        public ActorEntity(Guid actorId)
        {
            this.PartitionKey = ActorEntity.Key;
            this.RowKey = actorId.ToString();
        }

        public ActorEntity() { }

        public string Topology { get; set; }

        public string InQueue { get; set; }

        public string OutQueue { get; set; }

        public bool IsSpout { get; set; }
    }
}
