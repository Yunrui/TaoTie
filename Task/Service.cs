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
using System.Net;
using AzureAdapter;

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

        public string ErrorMessage
        {
            get;
            set;
        }

        public string ErrorStack
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
        /// sync object
        /// </summary>
        private static object LockObject = new Object();

        /// <summary>
        /// How many Tasks (Spout/Bolt) in a Service Role
        /// </summary>
        private const int ConcurrentTask = 10;

        /// <summary>
        /// ctor
        /// </summary>
        internal Service()
        {
            lock (LockObject)
            {
                // Born #ConcurrentTask threads for Tasks
                // and set state to NewBorn
                for (int i = 0; i < Service.ConcurrentTask; i++)
                {
                    ForkNewActor();
                }
            }
        }

        private void ForkNewActor()
        {
            // Each actors has its own Guid Identifier
            Guid id = Guid.NewGuid();
            Actor actor = new Actor() { State = ActorState.NewBorn, Id = id, DeploymentId = RoleEnvironment.DeploymentId, HeartBeat = DateTime.UtcNow };

            ThreadPool.QueueUserWorkItem(new WaitCallback(ThreadProc), actor);

            this.actors[id] = actor;
        }

        /// <summary>
        /// - Send Heartbeat to Central
        ///     - Service Up/Down
        ///     - Actor State
        /// - Kill Error Actor and Keep Actor Number;
        /// </summary>
        internal void Run()
        {
            do
            {
                Thread.Sleep(15000);

                Trace.TraceInformation("Service Maintain {0}", RoleEnvironment.DeploymentId);

                CloudTable table = StorageAccount.GetTable("topology");

                lock (LockObject)
                {
                    IList<Guid> errorActors = new List<Guid>();

                    // Remove Actor which in Error State, and fork a new one
                    foreach (Guid id in this.actors.Keys)
                    {
                        // NOTE: we still give Actor in Error State the last chance to report its state.
                        if (this.actors[id].State == ActorState.Error)
                        {
                            // Cannot modify this.actors in foreach
                            errorActors.Add(id);
                        }

                        try
                        {
                            if (this.actors[id].State == ActorState.Working)
                            {
                                var ass = GetAssignment(this.actors[id], false);

                                if (ass != null)
                                {
                                    if (string.Equals("Kill", ass.Operation, StringComparison.OrdinalIgnoreCase))
                                    {
                                        // I didn't lock this Actor considering there is no state transition from Working to non Error.
                                        this.actors[id].State = ActorState.Error;
                                    }
                                }
                            }

                            ActorAssignment assignment = new ActorAssignment(id) { HeartBeat = this.actors[id].HeartBeat, State = this.actors[id].State.ToString(), ErrorMessage = this.actors[id].ErrorMessage, ErrorStack = this.actors[id].ErrorStack};
                            TableOperation mergeOperation = TableOperation.InsertOrMerge(assignment);
                            TableResult retrievedResult = table.Execute(mergeOperation);

                            Trace.TraceInformation("Actor {0} updated HeartBeat", id);
                        }
                        catch (Exception e)
                        {
                            Trace.TraceInformation("Actor {0} failed to update HeartBeat due to {1}", id, e.Message);
                        }

                    }

                    foreach (Guid id in errorActors)
                    {
                        this.actors.Remove(id);
                        this.ForkNewActor();
                    }
                }
            }
            while (true);
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
                    if (assignment != null && !string.IsNullOrWhiteSpace(assignment.Name))
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
                        // $TODO: need to fetch package for new assignment
                        (new ActorExecution(actor, assignment)).Run();
                    }
                    catch(Exception e)
                    {
                        actor.State = ActorState.Error;
                        actor.ErrorMessage = e.Message;
                        actor.ErrorStack = e.StackTrace;
                        
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

        static ActorAssignment GetAssignment(Actor actor, bool prepareTestData = true)
        {
            ActorAssignment actorEntity = null;

            try
            {
                // $TEST: prepare test data so that the code can be executed in Azure Emulator
                if (prepareTestData)
                {
                    Environment.PrepareTestData(actor);
                }

                actorEntity = Assignment.GetAssignment(actor.Id.ToString());
            }
            catch (Exception e)
            {
                Trace.TraceInformation("Failed to get assignment:" + e.Message);
                RoundLogger.Current.Log(e.StackTrace);
            }

            return actorEntity;
        }
    }
}
