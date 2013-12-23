using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;
public class StorageAccount
{
    public static CloudStorageAccount GetAccount()
    {
        string settings = string.Empty;

        if (!RoleEnvironment.IsEmulated)
        {
            // Retrieve the storage account from the connection string.
            settings = CloudConfigurationManager.GetSetting("Microsoft.WindowsAzure.Plugins.Diagnostics.ConnectionString");
        }

        Microsoft.WindowsAzure.Storage.CloudStorageAccount storageAccount = null;

        if (string.IsNullOrWhiteSpace(settings))
        {
            storageAccount = Microsoft.WindowsAzure.Storage.CloudStorageAccount.DevelopmentStorageAccount;
        }
        else
        {
            // Retrieve the storage account from the connection string.
            storageAccount = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(settings);
        }
        return storageAccount;
    }

    public static CloudTable GetTable(string name)
    {
        Microsoft.WindowsAzure.Storage.CloudStorageAccount storageAccount = GetAccount();

        // Create the table client.
        CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

        CloudTable table = tableClient.GetTableReference(name);

        table.CreateIfNotExists();

        return table;
    }

    public static CloudQueue GetQueue(string name)
    {
        Microsoft.WindowsAzure.Storage.CloudStorageAccount storageAccount = GetAccount();

        CloudQueueClient client = storageAccount.CreateCloudQueueClient();

        CloudQueue queue = client.GetQueueReference(name);

        queue.CreateIfNotExists();

        return queue;
    }
}