using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using System.Net;

namespace CosmosDBLocalExample
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
        static void Main(string[] args)
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

        public async void OnStartAync() {
            this.cosmosClient = new CosmosClient(EndpointUri, PrimaryKey);
            await this.CreateDatabaseAsync();
            await this.CreateContainerAsync();
            //await this.CreateIndexingAsync();
            await this.AddItemsToContainerAsync();
            await this.QueryItemsAsync();
            //await ReplaceMessageItemAsync();
            //await DeleteMessageItemAsync();
            await CreateTTL();
        }
        public async Task CreateDatabaseAsync()
        {
            this.database = await this.cosmosClient.CreateDatabaseIfNotExistsAsync(this.databaseId);
            Console.WriteLine("Created Database: {0}\n", this.database.Id);
        }

        public async Task CreateContainerAsync()
        {
            ContainerProperties containerProperties = new ContainerProperties(this.containerId, "/MessageType");
            containerProperties.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/MessageId/*" });
            containerProperties.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/Properties/*" });
            //containerProperties.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/MessageType/*" });
            containerProperties.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/MessageQueue/*" });
            containerProperties.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/MessageType/*" });
            containerProperties.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/Location/*" });

            containerProperties.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/*" }); 
            this.container = (Container)await this.database.CreateContainerIfNotExistsAsync(containerProperties);
            //await this.database.CreateContainerIfNotExistsAsync(this.containerId, "/Location");
            Console.WriteLine("Created container: {0}\n", this.container.Id);
           
        }

        //public async Task CreateIndexingAsync() {
        //    ContainerResponse containerResponse = await this.cosmosClient.GetContainer(this.databaseId, this.containerId).ReadContainerAsync();
        //    containerResponse.Resource.IndexingPolicy.IndexingMode = IndexingMode.Consistent;
        //    containerResponse.Resource.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/*" });
        //    //containerResponse.Resource.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/Location" });
        //    //containerResponse.Resource.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/*" });
        //    await this.cosmosClient.GetContainer(this.databaseId, this.containerId).ReplaceContainerAsync(containerResponse.Resource);
        //}
        private async Task AddItemsToContainerAsync()
        {
            //Create a message object for the message 1

           Message message1 = new Message
           {
               MessageId = "e3a59b7e-a0a1-488a-923e-b32ed70bbe49",
               SessionId = "e3a59b7e-a0a1-488a-923e-b32ed70bbe49",
               InstanceId = "EmployeeCalculations",
               MessageType = "TMD001.TM.Test|message-server",
               MessageQueue = "CalculateMeNow",
               Description = "CalculateMeNow",
               Location = "mq://hostserver/interactiveserver/request",
               Timestamp = "2020-05-13T05:01:15.864-05:00",
               SendTime = null,
               SubmittedTime = "2020-05-13 05:01:14.162:00",
               MessageState = "Default",
               Properties = new Property
               {
                   TaskId = "a199278d-2bc4-4174-8b33-f28efed3eae0",
                   TaskCategory = null,
                   EmployeeId = "368",
                   PayPeriodType = "Current"
               }
           };
            await this.AddItemAsync(message1, this.container, message1.MessageType);

            // Create a message object for the message 1 
            Message message2 = new Message
            {
                MessageId = "0129945d-e6c0-40cf-beaa-f96697dcffff",
                SessionId = "0129945d-e6c0-40cf-beaa-f96697dcffff",
                InstanceId = "EmployeeCalculations:3638,Next",
                MessageType = "TMD002.TM.Test|message-server",
                MessageQueue = "CalculateMeNow",
                Description = "EmployeeCalculations",
                Location = "mq://hostserver/calculationserver/request",
                Timestamp = "2020-05-14T09:39:30.847-05:00",
                SendTime = null,
                SubmittedTime = "2020-05-14T09:39:30.284-05:00",
                MessageState = "Scheduled",
                Properties = new Property
                {
                    TaskId = "a199278d-2bc4-4174-8b33-f28efed3eae0",
                    TaskCategory = null,
                    EmployeeId = "3638",
                    PayPeriodType = "Next"
                },
                ttl = null
            };
            await this.AddItemAsync(message2, this.container, message2.MessageType);

            Message message3 = new Message
            {
                MessageId = "0129945d-e6c0-40cf-beaa-f96697dcfffg",
                SessionId = "0129945d-e6c0-40cf-beaa-f96697dcfffg",
                InstanceId = "EmployeeCalculations:3638,Next",
                MessageType = "TMD002.TM.Test|message-server",
                MessageQueue = "CalculateMeNow",
                Description = "EmployeeCalculations",
                Location = "mq://hostserver/calculationserver/request",
                Timestamp = "2020-05-14T09:39:30.847-05:00",
                SendTime = null,
                SubmittedTime = "2020-05-14T09:39:30.284-05:00",
                MessageState = "Scheduled",
                Properties = new Property
                {
                    TaskId = "a199278d-2bc4-4174-8b33-f28efed3eae0",
                    TaskCategory = null,
                    EmployeeId = "3639",
                    PayPeriodType = "Next"
                },
                ttl = null
            };
            var n = 10;
            while (n >= 0)
            {
                Message message4 = new Message
                {
                    MessageId = "0129945d-e6c0-40cf-beaa-f96697dcfffg"+n,
                    SessionId = "0129945d-e6c0-40cf-beaa-f96697dcfffg",
                    InstanceId = "EmployeeCalculations:3638,Next",
                    MessageType = "TMD002.TM.Test|message-server"+n,
                    MessageQueue = "CalculateMeNow",
                    Description = "EmployeeCalculations",
                    Location = "mq://hostserver/calculationserver/request",
                    Timestamp = "2020-05-14T09:39:30.847-05:00",
                    SendTime = null,
                    SubmittedTime = "2020-05-14T09:39:30.284-05:00",
                    MessageState = "Scheduled",
                    Properties = new Property
                    {
                        TaskId = "a199278d-2bc4-4174-8b33-f28efed3eae0",
                        TaskCategory = null,
                        EmployeeId = "3639",
                        PayPeriodType = "Next"
                    },
                    ttl = null
                };
                await this.AddItemAsync(message4,this.container, message4.MessageType);
                n--;
            }
                
        }

        private async Task AddItemAsync(Message message, Container container, string partitionKey) {
            ItemResponse<Message> messageResponse;
            try
            {
                // Read the item to see if it exists.  
                messageResponse = await container.ReadItemAsync<Message>(message.MessageId, new PartitionKey(partitionKey));
                //Console.WriteLine("Item in database with id: {0} already exists\n", messageResponse.Resource.MessageType);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                messageResponse = await container.CreateItemAsync<Message>(message, new PartitionKey(partitionKey));
                //Console.WriteLine("Created item in database with id: {0} Operation consumed {1} RUs.\n", messageResponse.Resource.MessageType, messageResponse.RequestCharge);
            }
        }
        private async Task QueryItemsAsync()
        {
            if (this.database == null)
                this.database = this.cosmosClient.GetDatabase(this.databaseId);
            if (this.container == null)
                this.container = this.database.GetContainer(this.containerId);
            //Query by Type
            var sqlQueryText = "SELECT * FROM c WHERE c.MessageType = 'TMD001.TM.Test|message-server'";

            Console.WriteLine("Running query: {0}\n", sqlQueryText);

            await this.ReadItemAsync(sqlQueryText, this.container);

            //Query by Queue
            var sqlQueryText1 = "SELECT * FROM c WHERE c.MessageType = 'TMD002.TM.Test|message-server' AND c.MessageQueue = 'CalculateMeNow'";

            Console.WriteLine("Running query: {0}\n", sqlQueryText1);

            await this.ReadItemAsync(sqlQueryText1, this.container);

            //Query by Properties
            var sqlQueryText2 = "SELECT * FROM c WHERE c.MessageType = 'TMD002.TM.Test|message-server' AND c.Properties.EmployeeId = '3638'";

            Console.WriteLine("Running query: {0}\n", sqlQueryText2);

            await this.ReadItemAsync(sqlQueryText2, this.container);
        }

        private async Task ReadItemAsync(string sqlQueryText, Container container) {
            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);

            FeedIterator<Message> queryResultSetIterator = container.GetItemQueryIterator<Message>(queryDefinition);

            List<Message> messages = new List<Message>();

            while (queryResultSetIterator.HasMoreResults)
            {
                FeedResponse<Message> currentResultSet = await queryResultSetIterator.ReadNextAsync();
                Console.WriteLine("Request Charge=" + currentResultSet.RequestCharge + " RUs");
                foreach (Message message in currentResultSet)
                {
                    messages.Add(message);
                    Console.WriteLine("\tRead: {0}\n", message.MessageId);
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
            messageResponse = await this.container.ReplaceItemAsync<Message>(itemBody, itemBody.MessageId, new PartitionKey(itemBody.MessageId));
            Console.WriteLine("Updated Message [{0}].\n \tBody is now: {1} {2} RUs\n", itemBody.Description, messageResponse.Resource, messageResponse.RequestCharge);
        }

        private async Task DeleteMessageItemAsync()
        {
            var partitionKeyValue = "mq://hostserver/calculationserver/request";
            var messageID = "0129945d-e6c0-40cf-beaa-f96697dcffff";
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

        private async Task CreateTTL()
        {
             
            Container tTLContainer;
            ContainerProperties containerProperties = new ContainerProperties("ExampleContainer2", "/Location");
            containerProperties.DefaultTimeToLive = -1;
            containerProperties.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/MessageId/*" });
            containerProperties.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/Properties/*" });
            containerProperties.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/MessageType/*" });
            containerProperties.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/MessageQueue/*" });
            //containerProperties.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/Location/*" });

            containerProperties.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/*" });
            tTLContainer = (Container)await this.database.CreateContainerIfNotExistsAsync(containerProperties);
            //await this.database.CreateContainerIfNotExistsAsync(this.containerId, "/Location");
            Console.WriteLine("Created container: {0}\n", this.container.Id);
            //tTLContainer = await this.database.CreateContainerIfNotExistsAsync(new ContainerProperties
            //{
            //    Id = "ExampleContainer2",
            //    PartitionKeyPath = "/Location",
            //    DefaultTimeToLive = -1 //(never expire by default)
            //});
            //Console.WriteLine("Created container: {0}\n", tTLContainer.Id);

            Message message = new Message
            {
                MessageId = "e3a59b7e-a0a1-488a-923e-b32ed70bbe39",
                SessionId = "e3a59b7e-a0a1-488a-923e-b32ed70bbe39",
                InstanceId = null,
                MessageType = "TMD001.TM.Test|message-server",
                MessageQueue = "CalculateMeNow",
                Description = "CalculateMeNow",
                Location = "mq://hostserver/interactiveserver/request",
                Timestamp = "2020-05-13T05:01:15.864-05:00",
                SendTime = null,
                SubmittedTime = "2020-05-13 05:01:14.162:00",
                MessageState = "Default",
                Properties = new Property
                {
                    TaskId = "a199278d-2bc4-4174-8b33-f28efed3eae0",
                    TaskCategory = null,
                    EmployeeId = "368",
                    PayPeriodType = "Current"
                },
                ttl = 20,
            };
            await this.AddItemAsync(message, tTLContainer, message.Location);
           
            var n = 11;
            while (n >= 0)
            {
                Message message4 = new Message
                {
                    MessageId = "0129945d-e6c0-40cf-beaa-f96697dcfffg" + n,
                    SessionId = "0129945d-e6c0-40cf-beaa-f96697dcfffg",
                    InstanceId = "EmployeeCalculations:3638,Next",
                    MessageType = "TMD002.TM.Test|message-server"+n,
                    MessageQueue = "CalculateMeNow",
                    Description = "EmployeeCalculations",
                    Location = "mq://hostserver/calculationserver/request" + n,
                    Timestamp = "2020-05-14T09:39:30.847-05:00",
                    SendTime = null,
                    SubmittedTime = "2020-05-14T09:39:30.284-05:00",
                    MessageState = "Scheduled",
                    Properties = new Property
                    {
                        TaskId = "a199278d-2bc4-4174-8b33-f28efed3eae0",
                        TaskCategory = null,
                        EmployeeId = "3639",
                        PayPeriodType = "Next"
                    },
                    ttl = null
                };
                await this.AddItemAsync(message4, tTLContainer, message4.Location);
                n--;
            }
            

            var sqlQueryText2 = "SELECT * FROM c WHERE c.MessageType = 'TMD001.TM.Test|message-server'";

                Console.WriteLine("Running query: {0}\n", sqlQueryText2);

                await this.ReadItemAsync(sqlQueryText2, tTLContainer);

            //Query by Queue
            var sqlQueryText1 = "SELECT * FROM c WHERE c.MessageType = 'TMD002.TM.Test|message-server' AND c.MessageQueue = 'CalculateMeNow'";

            Console.WriteLine("Running query: {0}\n", sqlQueryText1);

            await this.ReadItemAsync(sqlQueryText1, tTLContainer);
        }
    }
}
//test 1
//test 2
//test 3