using Microsoft.WindowsAzure.Storage.Queue;
using PrimitiveInterface;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Task
{
    class AzureQueueEmitter : IEmitter
    {
        private List<CloudQueue> queues = new List<CloudQueue>();
        private string schemaGroupingMode = string.Empty;

        public AzureQueueEmitter(string outQueues, string schemaGroupingMode)
        {
            this.schemaGroupingMode = schemaGroupingMode;

            var parts = outQueues.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);

            if (parts != null && parts.Length != 0)
            {
                foreach (string queue in parts)
                {
                    this.queues.Add(Environment.GetQueue(queue));
                }
            }
        }

        public void Emit(PrimitiveInterface.Tuple value)
        {
            if (this.queues.Count == 0)
            {
                throw new InvalidOperationException("This bolt doesn't have any output queue enabled.");
            }

            Random random = new Random();

            int index = 0;
            switch (this.schemaGroupingMode)
            {
                case "ShuffleGrouping":
                    index = random.Next(this.queues.Count);
                    this.queues[index].AddMessage(new CloudQueueMessage(value));
                    break;
                case "FieldGrouping":
                    index = Math.Abs(value.GetHashCode()) % this.queues.Count;
                    this.queues[index].AddMessage(new CloudQueueMessage(value));
                    break;
                case "AllGrouping":
                    foreach (CloudQueue queue in this.queues)
                    {
                        queue.AddMessage(new CloudQueueMessage(value));
                    }
                    break;
                default:
                    break;
            };
        }
    }
}
