using System;
using System.Collections.Generic;
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

namespace DQCompletness
{

    /// <summary>
    /// This is an example for TaoTie, make sure copy dll manually to approot
    /// </summary>
    [Export(typeof(ISpout))]
    public class DQLogSpout : ISpout
    {
        private IEmitter emitter;
        private CloudQueue queue;

        public DQLogSpout()
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse("DefaultEndpointsProtocol=http;AccountName=lqueue;AccountKey=vtVWSPvFXzJ3WzHcKpFbU9GY5YNsDGs493FMaxXpZFhwLN/pyfICpAOQcfj+QSP8T/r4yeIEHLOgKurPPB9EPQ==");
            CloudQueueClient client = storageAccount.CreateCloudQueueClient();
            this.queue = client.GetQueueReference("completeness");
        }

        public void Execute()
        {
            var messages = this.queue.GetMessages(32, TimeSpan.FromSeconds(30));

            foreach (var message in messages)
            {
                if (message == null)
                {
                    break;
                }

                var parts = message.AsString.Split(new string[] { "|#|" }, StringSplitOptions.None);

                if (parts.Length == 3)
                {
                    var tagID = parts[0];
                    if (String.Equals("171242", tagID, StringComparison.OrdinalIgnoreCase))
                    {
                        IList<string> strs = new List<string>()
                        {
                            message.AsString
                        };
                        this.emitter.Emit(new PrimitiveInterface.Tuple(strs));
                        this.queue.DeleteMessage(message);
                    }
                }
                else
                {
                    throw new ArgumentException(string.Format("The following message {0} is not valid.", message.AsString));
                }
            }
        }

        public void Open(IEmitter emitter, TopologyContext context)
        {
            this.emitter = emitter;
        }

        public IList<string> DeclareOutputFields()
        {
            return new List<string>() { "line" };
        }
    }
}
