namespace CFR
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.Table;

    public class Logger
    {
        private string prefix;
        private CloudTable table = null;

        public Logger(string prefix)
        {
            this.prefix = prefix;
        }

        public void Log(string message)
        {
            try
            {
                if (this.table == null)
                {
                    this.table = StorageAccount.GetTable("CFRCount");
                }

                TableEntity entity = new TableEntity();

                entity.PartitionKey = "CFRCount";
                entity.RowKey = string.Format("{0}_{1}", this.prefix, message);
                entity.ETag = "*";

                TableOperation operation = TableOperation.Insert(entity);
                table.Execute(operation);
            }
            catch (Exception)
            {
                // Ignore this log, consider retry later.
            }
        }
    }
}
