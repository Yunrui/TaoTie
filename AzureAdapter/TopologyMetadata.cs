using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureAdapter
{
    public class ActorMetadata : TableEntity
    {
        /// <summary>
        /// Suppose Topology Table is pretty small, so it's not necessary to Partition
        /// </summary>
        public const string Key = "Topology";

        public ActorMetadata(string name, string topology)
        {
            // $TODO: "-" is not allowed in name and topology

            this.PartitionKey = ActorMetadata.Key;
            this.Name = name;
            this.Topology = topology;
            this.RowKey = string.Format("{0}-{1}", topology, name);
            this.ETag = "*";
        }

        public ActorMetadata() { }

        public string Topology { get; set; }
        public string Name { get; set; }
        public string SchemaGroupingMode { get; set; }
        public string GroupingField { get; set; }
        public bool IsSpout { get; set; }
        public int ParallelCount { get; set; }
        public string Parent { get; set; }

        /// <summary>
        /// We pass all running queues, so not running means missing
        /// </summary>
        /// <param name="inQueues"></param>
        /// <returns></returns>
        public IList<string> GetMissingActors(IEnumerable<string> inQueues)
        {
            return this.GetInQueueList().Except(inQueues).ToList();
        }

        public IList<string> GetInQueueList()
        {
            IList<string> list = new List<string>();
            for (int i = 0; i < this.ParallelCount; i++)
            {
                list.Add(string.Format("{0}-{1}-{2}", this.Topology.ToLower(), this.Name.ToLower(), i));
            }
            return list;
        }
    }
    

    public class TopologyMetadata
    {
        private IList<ActorMetadata> actors = new List<ActorMetadata>();

        public string Name { get; set; }

        public IList<ActorMetadata> Actors
        {
            get { return this.actors; }
            set { this.actors = value; }
        }

        public int ActorRequired
        {
            get
            {
                return this.Actors.Sum(c => c.ParallelCount);
            }
        }

        public void DoAssignment(IList<ActorAssignment> assignments)
        {
            // It's possible that an actor is just assigned but not taken, so NewBorn is also a valid state
            var runningActors = assignments.Where(c => string.Equals(c.Topology, this.Name) && (c.State == "Working" || c.State == "NewBorn"));

            IList<ActorAssignment> list = new List<ActorAssignment>();

            foreach (ActorMetadata metadata in this.Actors)
            {
                // Verify what?
                // Only InQueue is enough
                var t = runningActors.Where(c => string.Equals(metadata.Name, c.Name)).Select(c => c.InQueue);
                foreach (string queue in metadata.GetMissingActors(t))
                {
                    ActorAssignment newAssignment = new ActorAssignment()
                        {
                            InQueue = queue,
                            OutQueues = this.GetOutQueues(metadata),
                            Topology = metadata.Topology,
                            Name = metadata.Name,
                            IsSpout = metadata.IsSpout,
                            SchemaGroupingMode = metadata.SchemaGroupingMode,
                            GroupingField = metadata.GroupingField,

                            // $NOTE: try to fix bug
                            State = "NewBorn",
                            HeartBeat = DateTime.UtcNow,
                        };

                    list.Add(newAssignment);
                }
            }

            // Do we have enough NewBorn workers for this assignment?
            // $NOTE, only check NewBorn is not enough,  because it's possbile not taken by actors
            var idleActors = assignments.Where(c => c.State == "NewBorn" && string.IsNullOrWhiteSpace(c.Name)).ToList();
            if (list.Count() <= idleActors.Count())
            {
                for (int i = 0; i < list.Count(); i++)
                {
                    list[i].RowKey = idleActors[i].RowKey;

                    TopologyBuilder.DoAssignment(list[i]);
                }
            }
            else
            {
                // $TODO: need log to notify there is no enough available resources
            }
        }

        /// <summary>
        /// Get Output queues' name from the next group of Bolt
        /// </summary>
        /// <param name="actor"></param>
        /// <returns></returns>
        public string GetOutQueues(ActorMetadata actor)
        {
            string queues = string.Empty;

            var nextBolts = this.Actors.Where(c => c.Parent == actor.Name);

            // To make sure, we have next round bolts group
            if (nextBolts.Count() > 0)
            {
                queues = string.Join(",", nextBolts.First().GetInQueueList());
            }

            return queues;
        }
    }
}
