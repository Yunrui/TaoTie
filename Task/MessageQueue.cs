using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Task
{
    class MessageQueue
    {
        private static object SyncObject = new object();

        // We make sure only a single queue instance in a process to share message bundle
        private static Dictionary<string, MessageQueue> queues = new Dictionary<string, MessageQueue>();

        private CloudQueue azureQueue = null;

        public static MessageQueue Get(string queue)
        {
            lock (MessageQueue.SyncObject)
            {
                if (!queues.ContainsKey(queue))
                {
                    queues[queue] = new MessageQueue(queue);
                }

                return queues[queue];
            }
        }

        public MessageQueue(string queue)
        {
            this.azureQueue = StorageAccount.GetQueue(queue);
        }

        public void AddMessage(PrimitiveInterface.Tuple tuple)
        {
            this.azureQueue.AddMessage(new CloudQueueMessage(tuple.GetSeriliableContent()));
        }
    }
}
