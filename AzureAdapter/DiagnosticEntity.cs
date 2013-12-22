using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureAdapter
{
    public class DiagnosticEntity : TableEntity
    {
                /// <summary>
        /// Suppose Topology Table is pretty small, so it's not necessary to Partition
        /// </summary>
        public const string Key = "Diagnostic";

        public DiagnosticEntity(string logId)
        {
            this.PartitionKey = DiagnosticEntity.Key;
            this.RowKey = logId;
            this.ETag = "*";
        }

        public string ErrorMessage { get; set; }

        public string Stack { get; set; }
    }
}
