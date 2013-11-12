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
        private CloudQueue queue = null;

        public AzureQueueEmitter(string outQueue)
        {
            if (!string.IsNullOrWhiteSpace(outQueue))
            {
                this.queue = Environment.GetQueue(outQueue);
            }
        }

        public void Emit(string value)
        {
            this.queue.AddMessage(new CloudQueueMessage(value));
        }
    }
}
