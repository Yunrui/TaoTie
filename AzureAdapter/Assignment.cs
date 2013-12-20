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
            // $TODO: cache table instance, it's proven to be slow operation
            CloudTable table = StorageAccount.GetTable("topology");

            ActorAssignment actorEntity = null;

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

        public static IList<ActorAssignment> GetRunningActors()
        {
            // $TODO: cache table instance, it's proven to be slow operation
            CloudTable table = StorageAccount.GetTable("topology");

            TableQuery<ActorAssignment> query = new TableQuery<ActorAssignment>().Where(TableQuery.GenerateFilterConditionForDate("HeartBeat", QueryComparisons.GreaterThan, DateTime.UtcNow.AddMinutes(-1)));
            return table.ExecuteQuery(query).ToList();
        }
    }
}
