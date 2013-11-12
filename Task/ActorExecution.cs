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
            if (this.assignment.IsSpout)
            {
                var spout = this.spouts.Where(c => c.GetType().Name == this.assignment.Name).FirstOrDefault() as ISpout;

                if (spout == null)
                {
                    // DLL is not dropped correctly, just error out
                    throw new InvalidOperationException(string.Format("Spout {0} cannot be loaded.", this.assignment.Name));
                }

                IEmitter emitter = new AzureQueueEmitter(this.assignment.OutQueues, this.assignment.SchemaGroupingMode, this.assignment.GroupingField, spout.DeclareOutputFields());
                spout.Open(emitter);
                do
                {
                    spout.Execute();

                    // $NOTE: whether it's too frequent to update this field?
                    this.actor.HeartBeat = DateTime.UtcNow;

                    // Release thread so that the other methods have a chance to be called
                    System.Threading.Thread.Sleep(1);
                }
                while (true);
            }
            else
            {
                CloudQueue inQueue = Environment.GetQueue(this.assignment.InQueue);

                var bolt = this.bolts.Where(c => c.GetType().Name == this.assignment.Name).FirstOrDefault() as IBolt;

                if (bolt == null)
                {
                    // DLL is not dropped correctly, just error out
                    throw new InvalidOperationException(string.Format("Bolt {0} cannot be loaded.", this.assignment.Name));
                }

                IEmitter emitter = new AzureQueueEmitter(this.assignment.OutQueues, this.assignment.SchemaGroupingMode, this.assignment.GroupingField, bolt.DeclareOutputFields());
                bolt.Open(emitter);
                do
                {
                    CloudQueueMessage message = inQueue.GetMessage();

                    if (message == null)
                    {
                        // Release thread so that the other methods have a chance to be called
                        System.Threading.Thread.Sleep(1);
                        continue;
                    }

                    // $TODO: need to convert from message to Tuple
                    bolt.Execute(PrimitiveInterface.Tuple.Parse(message.AsBytes));
                }
                while (true);
            }
        }
    }
}
