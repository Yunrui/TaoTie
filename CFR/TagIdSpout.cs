using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using PrimitiveInterface;
using System.Diagnostics;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;

namespace WordCountTopology
{
    /// <summary>
    /// This is an example for TaoTie, make sure copy dll manually to approot
    /// </summary>
    [Export(typeof(ISpout))]
    public class TagIdSpout : ISpout
    {
        private IEmitter emitter;
        private CloudQueue queue;

        public TagIdSpout()
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse("DefaultEndpointsProtocol=http;AccountName=lqueue;AccountKey=vtVWSPvFXzJ3WzHcKpFbU9GY5YNsDGs493FMaxXpZFhwLN/pyfICpAOQcfj+QSP8T/r4yeIEHLOgKurPPB9EPQ==");
            CloudQueueClient client = storageAccount.CreateCloudQueueClient();
            this.queue = client.GetQueueReference("cfr");
        }

        public void Execute()
        {
            var messages = this.queue.GetMessages(32, TimeSpan.FromSeconds(30));

            if (messages.Count() == 0)
            {
                // Let's sleep for a while since the queue is empty
                System.Threading.Thread.Sleep(10000);
            }

            foreach (var message in messages)
            {
                if (message == null)
                {
                    continue;
                }

                var parts = message.AsString.Split(new string[] { "|#|" }, StringSplitOptions.None);

                if (parts.Length != 7)
                {
                    throw new ArgumentException(string.Format("The following message {0} is not valid.", message.AsString));
                }

                var date = DateTime.Parse(parts[1]);
                IList<string> strs = new List<string>()
                    {
                        parts[0],
                        string.Format("{0}/{1}", date.Date.ToShortDateString(), date.Hour),
                        parts[4],
                        parts[5],
                    };

                this.emitter.Emit(new PrimitiveInterface.Tuple(strs));

                this.queue.DeleteMessage(message);
            }
        }

        public void Open(IEmitter emitter, TopologyContext context)
        {
            this.emitter = emitter;
        }

        public IList<string> DeclareOutputFields()
        {
            return new List<string>() { "tagId", "dateTime", "page", "location" };
        }
    }
}
