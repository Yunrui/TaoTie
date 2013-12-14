using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureAdapter
{
    public class Assignment
    {
        public static ActorAssignment GetAssignment(string key)
        {
            ActorAssignment actorEntity = null;

            // Create the CloudTable object that represents the "topology" table.
            CloudTable table = StorageAccount.GetTable("topology");

            // Create a retrieve operation that takes a customer entity.
            TableOperation retrieveOperation = TableOperation.Retrieve<ActorAssignment>(ActorAssignment.Key, key);

            // Execute the retrieve operation.
            TableResult retrievedResult = table.Execute(retrieveOperation);

            // Get Assignment
            if (retrievedResult.Result != null)
            {
                actorEntity = (ActorAssignment)retrievedResult.Result;
            }

            return actorEntity;
        }
    }
}
