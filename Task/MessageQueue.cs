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
        private const string delimiter = "__#__";

        /// <summary>
        /// This lock object is used to retrieve MessageQueue instance, so it's a static object in process
        /// </summary>
        private static object SyncObject = new object();

        /// <summary>
        /// This is used to keep sync in a message queue instance, which can be used by multiple threads
        /// </summary>
        private object inQueueSyncObject = new object();

        // We make sure only a single queue instance in a process to share message bundle
        private static Dictionary<string, MessageQueue> queues = new Dictionary<string, MessageQueue>();

        private CloudQueue azureQueue = null;
        private Queue<PrimitiveInterface.Tuple> waitingTuples = new Queue<PrimitiveInterface.Tuple>();
        private DateTime lastUpdateTime = DateTime.Now;

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

        public static IList<string> Parse(IEnumerable<CloudQueueMessage> azureMessages)
        {
            // $TODO: where to remove messages? how to do reliable message?
            IList<string> messages = new List<string>();

            foreach (CloudQueueMessage message in azureMessages)
            {
                foreach (var m in message.AsString.Split(new string[] { delimiter }, StringSplitOptions.None))
                {
                    messages.Add(m);
                }
            }

            return messages;
        }

        public MessageQueue(string queue)
        {
            this.azureQueue = StorageAccount.GetQueue(queue);
        }

        /// <summary>
        /// $TODO: We need consider adding a timeout, otherwise it's possible the last bundle will be delay
        /// </summary>
        /// <param name="tuple"></param>
        public void AddMessage(PrimitiveInterface.Tuple tuple)
        {
            lock (this.inQueueSyncObject)
            {
                this.waitingTuples.Enqueue(tuple);

                if ((DateTime.Now - this.lastUpdateTime).TotalSeconds > 1)
                {
                    long totalSize = 0;
                    List<String> messages = new List<string>();

                    while (this.waitingTuples.Count() > 0)
                    {
                        var nextMessage = this.waitingTuples.Peek().GetSeriliableContent();

                        // $NOTE: I thought it's UTF8, but still get > 64k exception, so might be unicode
                        totalSize += Encoding.Unicode.GetBytes(nextMessage).Length + 10;

                        // Leave 500 to message header
                        if (totalSize < CloudQueueMessage.MaxMessageSize - 500)
                        {
                            this.waitingTuples.Dequeue();
                            messages.Add(nextMessage);
                        }
                        else
                        {
                            this.azureQueue.AddMessage(new CloudQueueMessage(String.Join(delimiter, messages)));
                            totalSize = 0;
                            messages = new List<string>();
                        }
                    }

                    // Send the last group of messages also
                    this.azureQueue.AddMessage(new CloudQueueMessage(String.Join(delimiter, messages)));

                    this.lastUpdateTime = DateTime.Now;
                }
            }
        }
    }
}
