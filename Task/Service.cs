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
using Microsoft.WindowsAzure.Storage.Queue;
using System.ComponentModel.Composition;

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
        /// or we have to make sure only one thread can modify this dictionary
        /// </summary>
        private Dictionary<Guid, Actor> actors = new Dictionary<Guid, Actor>();

        /// <summary>
        /// How many Tasks (Spout/Bolt) in a Service Role
        /// </summary>
        private const int ConcurrentTask = 5;

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
            ActorAssignment assignment = null;
            Actor actor = (Actor)stateInfo;

            Trace.TraceInformation("Deployment {0} Actor {1} is Initialized.", actor.DeploymentId, actor.Id);
            
            while (true)
            {
                RoundLogger.Current.SetSceneId(actor);
                
                if (actor.State == ActorState.NewBorn)
                {
                    // Needs to contact central to get new assignment

                    // if there is an assignment then
                    assignment = GetAssignment(actor);
                    if (assignment != null)
                    {
                        actor.State = ActorState.Working;
                        RoundLogger.Current.Log("Get Assignment and switch State to Working.");
                    }
                    else
                    {
                        RoundLogger.Current.Log("Waiting for assignment.");

                        // Let's check assignment 10 seconds later
                        Thread.Sleep(10000);
                    } 
                }
                else if (actor.State == ActorState.Working)
                {
                    RoundLogger.Current.Log("Working on the assignment");                
                   
                    try
                    {
                        (new ActorExecution(actor, assignment)).Run();
                    }
                    catch(Exception e)
                    {
                        actor.State = ActorState.Error;
                        RoundLogger.Current.Log("Failed to execute topology:" + e.Message);
                        RoundLogger.Current.Log(e.StackTrace);
                    }
                }
                else if (actor.State == ActorState.Error)
                {
                    RoundLogger.Current.Log("The Actor is shutdown due to Error State.");
                    break;
                }

                actor.HeartBeat = DateTime.UtcNow;
            }
        }

        static ActorAssignment GetAssignment(Actor actor)
        {
            ActorAssignment actorEntity = null;

            try
            {
                // Create the CloudTable object that represents the "topology" table.
                CloudTable table = Environment.GetTable("topology");

                // $TEST: prepare test data so that the code can be executed in Azure Emulator
                Environment.PrepareTestData(actor);

                // Create a retrieve operation that takes a customer entity.
                TableOperation retrieveOperation = TableOperation.Retrieve<ActorAssignment>(ActorAssignment.Key, actor.Id.ToString());

                // Execute the retrieve operation.
                TableResult retrievedResult = table.Execute(retrieveOperation);

                // Get Assignment
                if (retrievedResult.Result != null)
                {
                    actorEntity = (ActorAssignment)retrievedResult.Result;
                    RoundLogger.Current.Log(string.Format("Get {0} Assignment from topology {1}, ", actorEntity.IsSpout ? "Spout" : "Bolt", actorEntity.Topology));
                }
            }
            catch (Exception e)
            {
                RoundLogger.Current.Log("Failed to get assignment:" + e.Message);
                RoundLogger.Current.Log(e.StackTrace);
            }

            return actorEntity;
        }
    }
}
