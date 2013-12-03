using Microsoft.WindowsAzure.Storage.Queue;
using PrimitiveInterface;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Task
{
    /// <summary>
    /// Azure Queue Emitter
    /// </summary>
    /// <remarks>
    /// How AzureQueue compares with ZeroMQ? 
    /// </remarks>
    class AzureQueueEmitter : IEmitter
    {
        private List<CloudQueue> queues = new List<CloudQueue>();
        private string schemaGroupingMode = string.Empty;
        private IList<string> groupingFields = null;
        private IList<string> declaredFields = null;

        /// <summary>
        /// ctor
        /// </summary>
        /// <param name="outQueues"></param>
        /// <param name="schemaGroupingMode"></param>
        /// <param name="groupingField"></param>
        public AzureQueueEmitter(string outQueues, string schemaGroupingMode, string groupingField, IList<string> declaredFields)
        {
            this.declaredFields = declaredFields;

            if (!string.IsNullOrEmpty(outQueues))
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

            if (!string.IsNullOrEmpty(groupingField))
            {
                this.groupingFields = groupingField.Split(new char[] { ',' }).ToList();
            }
        }

        /// <summary>
        /// Emit
        /// </summary>
        /// <param name="tuple"></param>
        public void Emit(PrimitiveInterface.Tuple tuple)
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
                    this.queues[index].AddMessage(new CloudQueueMessage(tuple.GetSeriliableContent()));
                    break;

                case "FieldGrouping":
                    StringBuilder distributedValue = new StringBuilder();
                    foreach (string filed in this.groupingFields)
                    {
                        distributedValue.Append(tuple.Get(this.declaredFields.IndexOf(filed)));
                    }

                    index = Math.Abs(distributedValue.ToString().GetHashCode()) % this.queues.Count;
                    this.queues[index].AddMessage(new CloudQueueMessage(tuple.GetSeriliableContent()));
                    break;

                case "AllGrouping":
                    foreach (CloudQueue queue in this.queues)
                    {
                        queue.AddMessage(new CloudQueueMessage(tuple.GetSeriliableContent()));
                    }
                    break;

                default:
                    break;
            };
        }
    }
}
