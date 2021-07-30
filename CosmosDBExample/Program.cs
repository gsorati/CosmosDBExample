using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;

namespace CosmosDBExample
{
    public class Program
    {
        // The Azure Cosmos DB endpoint for running this sample.
        private static readonly string EndpointUri = "https://localhost:8081/";
        // The primary key for the Azure Cosmos account.
        private static readonly string PrimaryKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

        // The Cosmos client instance
        private CosmosClient cosmosClient;

        // The database we will create
        private Database database;

        // The container we will create.
        private Container container;
        

        // The name of the database and container we will create
        private string databaseId = "ExampleDB1";
        private string containerId = "ExampleContainer1";
        public static void Main(string[] args)
        {
            try
            {
                Console.WriteLine("Beginning operations...\n");
                Program p = new Program();
                p.OnStartAync();

            }
            catch (CosmosException de)
            {
                Exception baseException = de.GetBaseException();
                Console.WriteLine("{0} error occurred: {1}", de.StatusCode, de);
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: {0}", e);
            }
            finally
            {
                Console.WriteLine("End of demo, press any key to exit.");
                Console.ReadKey();
            }
        }

        public async void OnStartAync()
        {
            // Create a new instance of the Cosmos Client
            this.cosmosClient = new CosmosClient(EndpointUri, PrimaryKey);
            await this.CreateDatabaseAsync();
            //Create container
            await this.CreateContainerAsync();
            //Add item to the container
            await this.AddItemsToContainerAsync();
            //read item to the container
            await this.QueryItemsAsync();

            //replace item to the container
            await ReplaceMessageItemAsync();
            
            //Delete item From the container
            await DeleteMessageItemAsync();

            //Create TTL fro 20 sec
            await CreateTTL();

        }

        public async Task CreateDatabaseAsync()
        {
            this.database = await this.cosmosClient.CreateDatabaseIfNotExistsAsync(this.databaseId);
            Console.WriteLine("Created Database: {0}\n", this.database.Id);
        }

        public async Task CreateContainerAsync()
        {
            this.container = await this.database.CreateContainerIfNotExistsAsync(this.containerId, "/MessageType");
            Console.WriteLine("Created container: {0}\n", this.container.Id);
            
        }

        private async Task AddItemsToContainerAsync()
        {
            // Create a message object for the message 1 
            Message message1 = new Message
            {
                MessageID = "1",
                MessageType = "Calculation",
                Description = "Calculation is running for the employee",
                MessageInformation = new MessageInfo { SendTime = "12:00:00", Location = "Hubli" }

            };
            
            try
            {
                // Read the item to see if it exists.  
                ItemResponse<Message> message1Response = await this.container.ReadItemAsync<Message>(message1.MessageID, new PartitionKey(message1.MessageType));
                Console.WriteLine("Item in database with id: {0} already exists\n", message1Response.Resource.MessageID);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                ItemResponse<Message> message1Response = await this.container.CreateItemAsync<Message>(message1, new PartitionKey(message1.MessageType));

                Console.WriteLine("Created item in database with id: {0} Operation consumed {1} RUs.\n", message1Response.Resource.MessageID, message1Response.RequestCharge);

            }

            // Create a message object for the message 1 
            Message message2 = new Message
            {
                MessageID = "2",
                MessageType = "LabourView",
                Description = "LabourView process is running for the employee",
                MessageInformation = new MessageInfo { SendTime = "10:00:00", Location = "Banglore" }

            };

            try
            {
                // Read the item to see if it exists.  
                ItemResponse<Message> message2Response = await this.container.ReadItemAsync<Message>(message2.MessageID, new PartitionKey(message2.MessageType));
                Console.WriteLine("Item in database with id: {0} already exists\n", message2Response.Resource.MessageID);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                ItemResponse<Message> message2Response = await this.container.CreateItemAsync<Message>(message2, new PartitionKey(message2.MessageType));

                Console.WriteLine("Created item in database with id: {0} Operation consumed {1} RUs.\n", message2Response.Resource.MessageID, message2Response.RequestCharge);
            }

        }

        private async Task QueryItemsAsync()
        {
            var sqlQueryText = "SELECT * FROM c WHERE c.MessageType = 'Calculation'";

            Console.WriteLine("Running query: {0}\n", sqlQueryText);

            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);
            if (this.database == null)
                this.database = this.cosmosClient.GetDatabase(this.databaseId);
            if (this.container == null)
                this.container = this.database.GetContainer(this.containerId);
            FeedIterator<Message> queryResultSetIterator = this.container.GetItemQueryIterator<Message>(queryDefinition);
            
            List<Message> messages = new List<Message>();

            while (queryResultSetIterator.HasMoreResults)
            {
                FeedResponse<Message> currentResultSet = await queryResultSetIterator.ReadNextAsync();
                Console.WriteLine("Request Charge="+currentResultSet.RequestCharge + " RUs");
                foreach (Message message in currentResultSet)
                {
                    messages.Add(message);
                    Console.WriteLine("\tRead {0}\n", message);
                }
            }




            var sqlQueryText1 = "SELECT * FROM c WHERE c.MessageId = '1'";

            Console.WriteLine("Running query: {0}\n", sqlQueryText1);

            QueryDefinition queryDefinition1 = new QueryDefinition(sqlQueryText1);
            
            FeedIterator<Message> queryResultSetIterator1 = this.container.GetItemQueryIterator<Message>(queryDefinition1);

            List<Message> messages1 = new List<Message>();

            while (queryResultSetIterator1.HasMoreResults)
            {
                FeedResponse<Message> currentResultSet = await queryResultSetIterator1.ReadNextAsync();
                Console.WriteLine("Request Charge=" + currentResultSet.RequestCharge+ " RUs" + " diagnostic="+currentResultSet.Diagnostics + " ETag=" + currentResultSet.ETag);
                foreach (Message message in currentResultSet)
                {
                    messages1.Add(message);
                    Console.WriteLine("\tRead {0}\n", message);
                }
            }
        }
        private async Task ReplaceMessageItemAsync()
        {
            if (this.database == null)
                this.database = this.cosmosClient.GetDatabase(this.databaseId);
            if (this.container == null)
                this.container = this.database.GetContainer(this.containerId);
            ItemResponse<Message> messageResponse = await this.container.ReadItemAsync<Message>("1", new PartitionKey("Calculation"));
            var itemBody = messageResponse.Resource;

            // update registration status from false to true
            itemBody.Description = "Calculation is running for the employee";

            // replace the item with the updated content
            messageResponse = await this.container.ReplaceItemAsync<Message>(itemBody, itemBody.MessageID, new PartitionKey(itemBody.MessageType));
            Console.WriteLine("Updated Message [{0}].\n \tBody is now: {1} {2} RUs\n", itemBody.Description, messageResponse.Resource, messageResponse.RequestCharge);
        }

        private async Task DeleteMessageItemAsync()
        {
            var partitionKeyValue = "Calculation";
            var messageID = "1";
            ItemResponse<Message> messageResponse;

            // Delete an item. Note we must provide the partition key value and id of the item to delete
            messageResponse = await this.container.DeleteItemAsync<Message>(messageID, new PartitionKey(partitionKeyValue));
            Console.WriteLine("Deleted Message: [{0},{1},{2} RUs]\n", partitionKeyValue, messageID, messageResponse.RequestCharge);

            //partitionKeyValue = "LabourView";
            //messageID = "2";

            //// Delete an item. Note we must provide the partition key value and id of the item to delete
            //wakefieldFamilyResponse = await this.container.DeleteItemAsync<Message>(messageID, new PartitionKey(partitionKeyValue));
            //Console.WriteLine("Deleted Family [{0},{1}]\n", partitionKeyValue, messageID);
        }

        private async Task CreateTTL() {
            Container tTLContainer;
            tTLContainer = await this.database.CreateContainerIfNotExistsAsync(new ContainerProperties
            {
                Id = "ExampleContainer2",
                PartitionKeyPath = "/MessageType",
                DefaultTimeToLive = -1 //(never expire by default)
            });
            Console.WriteLine("Created container: {0}\n", tTLContainer.Id);

            Message message = new Message
            {
                MessageID = "3",
                MessageType = "TimeToLive",
                Description = "TimeToLive Calculation is running for the employee",
                MessageInformation = new MessageInfo { SendTime = "12:00:00", Location = "Hubli" },
                ttl = 20

            };
            ItemResponse<Message> messageResponse;
            try
            {
                // Read the item to see if it exists.  
                messageResponse = await tTLContainer.ReadItemAsync<Message>(message.MessageID, new PartitionKey(message.MessageType));
                Console.WriteLine("Item in database with id: {0} already exists\n", messageResponse.Resource.MessageID);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                // Create an item in the container representing the Andersen family. Note we provide the value of the partition key for this item, which is "Andersen"
                messageResponse = await tTLContainer.CreateItemAsync<Message>(message, new PartitionKey(message.MessageType));

                // Note that after creating the item, we can access the body of the item with the Resource property off the ItemResponse. We can also access the RequestCharge property to see the amount of RUs consumed on this request.
                Console.WriteLine("Created item in database with id: {0} Operation consumed {1} RUs.\n", messageResponse.Resource.MessageID, messageResponse.RequestCharge);

            }
        }
    }
}
