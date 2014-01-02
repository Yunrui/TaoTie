﻿using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using PrimitiveInterface;
using System.Diagnostics;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using System.Globalization;
using AzureAdapter;
using Microsoft.WindowsAzure.ServiceRuntime;

namespace CFR
{
    /// <summary>
    /// This is an example for TaoTie, make sure copy dll manually to approot
    /// </summary>
    [Export(typeof(ISpout))]
    public class TagIdSpout : ISpout
    {
        private const string QueueName = "o365";
        private const string SeperatorString = "|__|";
        private const string SeperatorStringForContext = "|$$|";
        private const char SeperatorStringForMetaData = '=';

        private string SymbolOfPageName = "report=";

        private string[] CFRTagIDs = new string[20]{"7257", "7261", "8553", "8554", "8555", "8661", "8819",
            "8820", "9097", "9098", "9099", "9100", "9101", "9102", "9103", "9217", "9286", "20629", "20977", "171242"};

        private IEmitter emitter;
        private CloudQueue queue;

        private Logger logger;

        long count = 0;

        public TagIdSpout()
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse("DefaultEndpointsProtocol=http;AccountName=lqueue;AccountKey=vtVWSPvFXzJ3WzHcKpFbU9GY5YNsDGs493FMaxXpZFhwLN/pyfICpAOQcfj+QSP8T/r4yeIEHLOgKurPPB9EPQ==");
            CloudQueueClient client = storageAccount.CreateCloudQueueClient();
            this.queue = client.GetQueueReference(QueueName);
        }

        public void Execute()
        {
            IEnumerable<CloudQueueMessage> messages = null;
            if (!RoleEnvironment.IsEmulated)
            {
                messages = this.queue.GetMessages(32, TimeSpan.FromDays(7));
            }
            else
            {
                /* Fake local data */
                var list = new List<CloudQueueMessage>();

                list.Add(new CloudQueueMessage("11:26:2013::00:03:57:984|__|52|__|1|__|8819|__||__||__||__||__|2396|__||$$|ContextClass=Microsoft.Online.BOX.Util.Logging.BoxUIData|$$|CID=d2ae567f-8cd1-49e1-8720-041f3faa5da0|$$|SID=b20f5b90-069b-4174-8992-092358b28913|$$|TID=|$$|TC=|$$|UID=|$$|UPN=|$$|IP=<PII:H101(1ttQ6rUNPnakrZX7awWWigS7uzjhQgzot0JX1ldKWmw=):E102(BtDGTbWe+Uz5F8YCdMNBah6TsjICCeStYulWCDnUtG2xpSSjRVuJ1xnKWQkMCzx9w7yZMuZ9dRKrVG3KJsMKrq5yInSSC0YXn+TU/7phw9a9wheKb+3xYvvrFUHQy55oGhmpJku+L0WJno9uFlYZ4XkWW4SyOVdtWyO+wduAquVWtZuBTBsTXHTVa+PaMVxJYSq0lvNSIkivIF+voDRE7U2vJlfaYN8RKZ5XIYl6N9NEeT1sLWyEKzW4HLfdIu3qv6SP7rECUQCo/R8M1RYMDwpLiUSiScSUSTQG8HB+m8xUu1Wv+CL4zlgVfKg5vRGE5+/yUOXDxC9ywo1+X3q+eg==):G(jp00230088)>|$$|Diag=<!-- Server IP: 10.62.106.62 ; Instance: Website_IN_3; DeploymentId: e6868b4c64554efdb2cc5b0d91f756df -->|$$|PN=/GetChart.chart?size=Medium&report=StaleMailbox&serviceId=Exchange&categoryId=Mail&cid=da6b953c-7588-40de-afbd-1269fe4e01f5|$$|BR=IE10.0|$$|OS=Windows 7|$$|F=15GA|$$|R=WorldWide|$$|SK=|$$|TT=CustomerTraffic|$$|FA=|$$|FS=|$$||__|e6868b4c64554efdb2cc5b0d91f756df|__|IN_3"));

                list.Add(new CloudQueueMessage("11:26:2013::00:03:34:006|__|121|__|1|__|171242|__||__||__||__||__|4340|__||$$|ContextClass=Microsoft.Online.BOX.Util.Logging.BoxUIData|$$|CID=7805b676-c21d-4f7a-9ad6-76ff3d324452|$$|SID=833fdadc-cf1a-4c00-b5f7-cce4e4d5c7cf|$$|TID=f69e53bf-b664-4172-8618-0d8e7f8129fc|$$|TC=JP|$$|UID=<PII:H101(WuCCNQ4CPhOKYIKVC6MArqnJA/cCgBmKeEQUd7VjpwU=):E102(M2f5bRNbJIKUsp2exxD7n53Vdp3lr4K9kIbpvIK4/W+Ozeg2GJ7s66NSKiZvEdkbe6JYGSLCmzquRbzbosBxD3QXUAVUAaqvEW1g+2xI+yXQSRvmRZcscHLu8fpAAvAOVcChA4hEjH9zZHD/HEZrmosJVvCgFyr9RyngSf9FwYxpE8wms3ytY9HI65BnRhVlIoeWLjn5mBMSwuJja2pZ83LqiRXokjFrWZfeNAT5hO6kg5YZMTqxpwq3p3Dt7FrMsLfRHF/banu21Tmz1UwJvqIubxSxhxf/gkip5cM+4w5gjKv81M0XFoWMG/kdo62VYKatpP11uIvjLs05xVIoKA==)>|$$|UPN=|$$|IP=<PII:H101(9XDCv1lxJoVAhLIcKnMjPmpXAbP7EAewHgzgWwmc7q0=):E102(ec18w2CSCX9Ilq3FTxuSFumChE6EUoXQMcT1czABpRBqdc64hn7aCixPnAwMA0B26LEpZBpMLD2vMl8ZXqT20SogHS60ggnwqII6OcLvQ3uhv3gTbybvMzcWqwYX2QBJ5w25ZEjEpu3ehvPzH4r84Qxs5iRgp77O9heI1Uf4zU3CxwnghngE/WYKmPr9euyzzpMOxjf6bkjROwi97ZGywNvZGCIvjqLIR3+vqg3K91Gdq2/xDNZXwSkclCf5//P6Yid708AmR3odPjmDtWKktnRcwTtSRmQ3xJT8AVuRS88LcJFFVOMLEnilgP1WmTX3l88gjw+PiHTsaNLFLJCtBw==):G(jp00230088)>|$$|Diag=<!-- Server IP: 10.62.102.56 ; Instance: Website_IN_2; DeploymentId: e6868b4c64554efdb2cc5b0d91f756df -->|$$|PN=/Microsoft.Online.BOX.Admin.UI.Admin.EnterpriseHome.GetReportMailProtection.model?format=json|$$|BR=IE9.0|$$|OS=Windows 7|$$|F=PasswordRecoveryInfoPage;Exp9015C|$$|R=WorldWide|$$|SK=E|$$|TT=CustomerTraffic|$$|FA=|$$|FS=|$$||$$|ContextClass=AdHoc|$$|0=ReportName:MailTraffic, TenantGuid:f69e53bf-b664-4172-8618-0d8e7f8129fc, TenantName:shinko.onmicrosoft.com, StartDate:2013-11-19 00:00:00, EndDate:2013-11-25 00:03:31, ReportType:Summary, ExpectRowsCountDaily:1, XAxisCount:7, YAxisCount:5, PointsCount:35|$$||$$||__|e6868b4c64554efdb2cc5b0d91f756df|__|IN_2"));

                messages = list;
            }

            if (messages.Count() == 0)
            {
                // Let's sleep for a while since the queue is empty
                System.Threading.Thread.Sleep(10000);
            }

            foreach (var message in messages)
            {
                try
                {
                    if (message == null)
                    {
                        continue;
                    }

                    /*
                     * TimeStamp|__|ThreadId|__|Severity|__|TagId|__|Class|__|Method|__|Message|__|Exception|__|ProcessId|__|Context
                     * 11:26:2013::00:03:32:917|__|124|__|0|__|4023|__||__||__||__||__|4340|__|
                     * |$$|ContextClass=Microsoft.Online.BOX.Util.Logging.BoxUIData|$$|CID=29c16174-348c-434c-9faf-33b28df73444|$$|SID=292500b2-4581-4c03-a01a-a02b669f7115|$$|TID=|$$|TC=|$$|UID=|$$|UPN=|$$|IP=<PII:H101(J7tlUG8xXZyOGhlLwEsbK0wW+l5Ic7VUwhdFQRCgg/c=):E102(iaCFU51bRcIGvuglAFJlsswOIQtGi8AfUnb7p8W+SUMImzY7niOYIynQ4IgouHQvTbseBtysSfYL4JFI13ruOGVzT7mnI7ejm5/fjddJUPDsTHOJXoc9J3SxIsrpYzH9OXnfs4V5bCKCY5XowNkmyPY+aLIk/gvXcLBu5r3xY01y4/yEZYR5sB9AKatgClz5xXNTH5QRr4jfsiXade/yPVNwuK3SFYMXCQYMz2J1xfgLa6u+uZlH7jd1rMPB8bvKAypQg2oi4tOEdD2VAN7ORZulXIID4ngZuRFDngsUte6pxECtzrNhSuKuXtzxbEO6pR2n+srhZ+ylMjnRzkv00w==):G(auFFDA0091)>|$$|Diag=<!-- Server IP: 10.62.102.56 ; Instance: Website_IN_2; DeploymentId: e6868b4c64554efdb2cc5b0d91f756df -->|$$|PN=/Admin/Initialize.ajax|$$|BR=IE10.0|$$|OS=Windows 8.1|$$|F=15GA|$$|R=WorldWide|$$|SK=|$$|TT=CustomerTraffic|$$|FA=|$$|FS=|$$||$$|ContextClass=AdHoc|$$|0=Initialize|$$|1=Microsoft.Online.BOX.Admin.UI.WebControls.AssistancePanel|$$|2=BOX.Admin.UI, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null|$$||$$||__|e6868b4c64554efdb2cc5b0d91f756df|__|IN_2
                     */

                    var parts = message.AsString.Split(new string[] { SeperatorString }, StringSplitOptions.None);

                    if (parts.Length != 12)
                    {
                        Trace.TraceInformation(string.Format("The following message {0} is not valid.", message.AsString));
                    }
                    else if (CFRTagIDs.Contains(parts[3])) // only the logs for cfr wil be consumered.
                    {
                        DateTime date = DateTime.ParseExact(parts[0], "MM:dd:yyyy::HH:mm:ss:fff", CultureInfo.InvariantCulture);

                        var contextParts = parts[9].Split(new string[] { SeperatorStringForContext }, StringSplitOptions.None);

                        if (contextParts.Length < 11)
                        {
                            Trace.TraceInformation(string.Format("The following message {0} is not valid.", message.AsString));
                        }
                        else
                        {
                            DQLogProcessor processor = null;
                            if (string.Equals("171242", parts[3], StringComparison.OrdinalIgnoreCase))
                            {
                                processor = new DQLogProcessor(contextParts[21].Substring(2));
                            }

                            IList<string> strs = new List<string>()
                            {
                                parts[3], // tagid
                                string.Format("{0}/{1}", date.Date.ToShortDateString(), date.Hour), //date
                                this.ParserName(this.ParserContext(contextParts[10])), // page name
                                this.ParserContext(contextParts[5]), // tc
                                processor != null ? processor.TenantName : string.Empty,
                                processor != null ? processor.ReportName : string.Empty,
                                processor != null ? processor.GetCompletness(date).ToString() : string.Empty
                            };

                            this.emitter.Emit(new PrimitiveInterface.Tuple(strs));
                        }
                    }
                }
                catch (Exception ex)
                {
                    Trace.TraceInformation(string.Format("Message Parser Error : {0}, {1}, {2}", ex.Message, ex.StackTrace, message));
                }

                //this.queue.DeleteMessage(message);

                count++;

                if (count % 20000 == 0)
                {
                    logger.Log(count.ToString());
                }
            }
        }

        public void Open(IEmitter emitter, TopologyContext context)
        {
            this.emitter = emitter;

            this.logger = new Logger(context.ActorId);
        }

        public IList<string> DeclareOutputFields()
        {
            return new List<string>() { "tagId", "dateTime", "page", "location", "tenantName", "report", "completness"};
        }

        private string ParserContext(string context)
        {
            string value = string.Empty;

            if (!string.IsNullOrEmpty(context))
            {
                int valueStartPosition = context.IndexOf(SeperatorStringForMetaData) + 1;

                if (valueStartPosition >= 1 && valueStartPosition < context.Length)
                {
                    value = context.Substring(valueStartPosition);
                }
            }

            return value;
        }

        /// <summary>
        /// Get the value of report page name
        /// </summary>
        /// <param name="context">PN=/GetChart.chart?size=Medium&report=StaleMailbox&serviceId=Exchange&categoryId=Mail&cid=da6b953c-7588-40de-afbd-1269fe4e01f5</param>
        /// <returns></returns>
        private string ParserName(string context)
        {
            string pageName = string.Empty;

            int startPosition = context.IndexOf(SymbolOfPageName) + SymbolOfPageName.Length;

            if (startPosition >= SymbolOfPageName.Length && startPosition < context.Length)
            {
                int endPosition = context.IndexOf("&", startPosition);

                if (endPosition > startPosition)
                {
                    pageName = context.Substring(startPosition, endPosition - startPosition);
                }
            }

            return pageName;
        }
    }
}
