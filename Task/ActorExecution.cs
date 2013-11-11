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
            if (assignment.IsSpout)
            {
            }
            else
            {
            }
            

            /*
            CloudQueue queue = Environment.GetQueue(this.assignment.InQueue);
            CloudQueueMessage message = queue.GetMessage();
             * */
        }
    }
}
