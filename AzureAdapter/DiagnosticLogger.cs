using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureAdapter
{
    public class DiagnosticLogger
    {
        private string prefix = string.Empty;
        private CloudTable table = null; 

        /// <summary>
        /// Each instance should have its own prefix to distinguish others
        /// </summary>
        /// <param name="prefix"></param>
        public DiagnosticLogger(string prefix)
        {
            this.prefix = prefix;
        }

        public void Log(Exception exception)
        {
            try
            {
                if (this.table == null)
                {
                    this.table = StorageAccount.GetTable("diagnostic");
                }

                DiagnosticEntity entity = new DiagnosticEntity(string.Format("{0}-{1}", this.prefix, Guid.NewGuid()));
                entity.ErrorMessage = exception.Message;
                entity.Stack = exception.StackTrace;
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
