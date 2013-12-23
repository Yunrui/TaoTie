using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.ComponentModel.Composition.Hosting;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Queue;
using PrimitiveInterface;
using System.Diagnostics;
using AzureAdapter;

namespace Task
{
    class ActorExecution
    {
        private Actor actor;
        private ActorAssignment assignment;

        [ImportMany(typeof(ISpout))]
        private IEnumerable<ISpout> spouts;

        [ImportMany(typeof(IBolt))]
        private IEnumerable<IBolt> bolts;


        public ActorExecution(Actor actor, ActorAssignment assignment)
        {
            this.actor = actor;
            this.assignment = assignment;
        }

        public void Run()
        {
            Stopwatch watch = new Stopwatch();

            //An aggregate catalog that combines multiple catalogs
            var catalog = new AggregateCatalog();
            //Adds all the parts found in all assemblies in 
            //the same directory as the executing program
            catalog.Catalogs.Add(new DirectoryCatalog(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location)));

            //Create the CompositionContainer with the parts in the catalog
            CompositionContainer container = new CompositionContainer(catalog);

            //Fill the imports of this object
            container.ComposeParts(this);

            // Find right Actor based on assignment
            if (this.assignment.IsSpout.HasValue && this.assignment.IsSpout.Value)
            {
                var spout = this.spouts.Where(c => c.GetType().Name == this.assignment.Name).FirstOrDefault() as ISpout;

                if (spout == null)
                {
                    // DLL is not dropped correctly, just error out
                    throw new InvalidOperationException(string.Format("Spout {0} cannot be loaded.", this.assignment.Name));
                }

                IEmitter emitter = new MessageEmitter(this.assignment.OutQueues, this.assignment.SchemaGroupingMode, this.assignment.GroupingField, spout.DeclareOutputFields());
                spout.Open(emitter, new TopologyContext() { ActorId = actor.Id.ToString(), });
                do
                {
                    watch.Restart();
                    spout.Execute();
                    watch.Stop();

                    RoundLogger.Current.Log(string.Format("The Spout {0} takes {1} ms for this round.", assignment.Name, watch.ElapsedMilliseconds));

                    // $NOTE: whether it's too frequent to update this field?
                    this.actor.HeartBeat = DateTime.UtcNow;

                    if (this.actor.State == ActorState.Error)
                    {
                        break;
                    }

                    // Release thread so that the other methods have a chance to be called
                    System.Threading.Thread.Sleep(1);
                }
                while (true);
            }
            else
            {
                var bolt = this.bolts.Where(c => c.GetType().Name == this.assignment.Name).FirstOrDefault() as IBolt;

                if (bolt == null)
                {
                    // DLL is not dropped correctly, just error out
                    throw new InvalidOperationException(string.Format("Bolt {0} cannot be loaded.", this.assignment.Name));
                }

                IEmitter emitter = new MessageEmitter(this.assignment.OutQueues, this.assignment.SchemaGroupingMode, this.assignment.GroupingField, bolt.DeclareOutputFields());
                bolt.Open(emitter, new TopologyContext() { ActorId = actor.Id.ToString(), });

                CloudQueue inQueue = StorageAccount.GetQueue(this.assignment.InQueue);
                do
                {
                    watch.Restart();

                    var messages = inQueue.GetMessages(32, TimeSpan.FromDays(7));

                    if (messages.Count() == 0)
                    {
                        // We stop watch first so that sleeping time not count into this round
                        watch.Stop();
                        System.Threading.Thread.Sleep(10000);
                    }
                    else
                    {
                        foreach (var message in MessageQueue.Parse(messages))
                        {
                            if (message == null)
                            {
                                continue;
                            }

                            bolt.Execute(PrimitiveInterface.Tuple.Parse(message));

                            /*
                             * $EXPERIMENT: can we not delete messages (which is very slow), but set visiblityTimeOut to 7days?
                             * Retention can remvoe it immediately without perf penalty on looking up?
                            try
                            {
                                inQueue.DeleteMessage(message);
                            }
                            catch (Exception)
                            {
                                // ignore any exceptions
                                // Let's make sure infra can handle all exception cases so that this bolt won't be stopped by it
                            }
                             * */
                        }

                        watch.Stop();
                    }

                    RoundLogger.Current.Log(string.Format("The Bolt {0} takes {1} ms for this round.", assignment.Name, watch.ElapsedMilliseconds));

                    // $NOTE: whether it's too frequent to update this field?
                    this.actor.HeartBeat = DateTime.UtcNow;

                    if (this.actor.State == ActorState.Error)
                    {
                        break;
                    }
                }
                while (true);
            }
        }
    }
}
