using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading.Tasks;

namespace CosmosDBExampleDemo
{


    class Program
    {
        #region Properties
        private static readonly string EndpointUri = "https://localhost:8081/";//"https://azure-cosmos-gadi.documents.azure.com:443/";//
        private static readonly string PrimaryKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";//"ZDHZXBLgDxS6zWmVTU1fWD2YHqmHCLSH1O90qtu3rESJ1UxunUsBpEHrXwEaJs2bRVmcIgyIKkEvupm6t5OwrA==";//
        private CosmosClient cosmosClient;
        private Database database;
        private Container containerWithDefaultIndex;
        private Container containerWithIndex;
        private Container messageTrackingIndex;
        private string databaseId = "CosmosDemoDB11";

        private double timeTakenInMs = 0;
        private int recordCount = 0;
        private double requestCharges = 0;
        private bool isSingleRecord= false;
        #endregion

        #region methods
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
                //Console.WriteLine("End of demo, press any key to exit.");
                Console.ReadKey();
            }
        }
        public async void OnStartAync()
        {
            this.cosmosClient = new CosmosClient(EndpointUri, PrimaryKey);
            await this.CreateDatabaseAsync(this.databaseId);
            await this.CreateContainerAndItemForMessageTrackingIndex();
            await this.CreateContainerAndItemWithDefaultIndexAsync();
            await this.CreateContainerAndItemWithIndexAsync();
            await this.QueryItemsAsync();
            Console.WriteLine("\n----------------------Stored procedure: Query by MessageType with indexing|Partition Key-TenantID---------------------------");
            await this.QueryByStoredProcedureAsync();

        }

        public async Task CreateDatabaseAsync(string databaseId)
        {
            this.database = await this.cosmosClient.CreateDatabaseIfNotExistsAsync(databaseId);
            Console.WriteLine("Created Database: {0}\n", database.Id);

        }
        private async Task CreateContainerAndItemForMessageTrackingIndex()
        {
            //CONTAINER WITHOUT INDEX POLICY
            ContainerProperties containerProperties = new ContainerProperties("ContainerMessageTrackingIndex", "/TenantId");
            containerProperties.DefaultTimeToLive = -1;

            this.messageTrackingIndex = (Container)await this.database.CreateContainerIfNotExistsAsync(containerProperties);
            Console.WriteLine("Created container: {0}\n", this.messageTrackingIndex.Id);

        }
        public async Task CreateContainerAndItemWithDefaultIndexAsync()
        {
            //CONTAINER WITHOUT INDEX POLICY
            ContainerProperties containerProperties = new ContainerProperties("ContainerWithDefaultIndex", "/TenantId");
            containerProperties.DefaultTimeToLive = -1;

            this.containerWithDefaultIndex = (Container)await this.database.CreateContainerIfNotExistsAsync(containerProperties);
            Console.WriteLine("Created container: {0}\n", this.containerWithDefaultIndex.Id);

            await this.AddItemsToContainerAsync(this.containerWithDefaultIndex);
        }
        public async Task CreateContainerAndItemWithIndexAsync()
        {
            //CONTAINER WITH INDEX POLICY(MessageQueue,Properties,MessageType)
            ContainerProperties containerProperties = new ContainerProperties("ContainerWithIndex", "/TenantId");
            containerProperties.DefaultTimeToLive = -1;
            containerProperties.IndexingPolicy.IndexingMode = IndexingMode.Consistent;
            containerProperties.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/MessageQueue/*" });
            containerProperties.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/Properties/*" });
            containerProperties.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/MessageType/*" });
            containerProperties.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/*" });

            this.containerWithIndex = (Container)await this.database.CreateContainerIfNotExistsAsync(containerProperties);
            Console.WriteLine("\nCreated container: {0}\n", this.containerWithIndex.Id);

            await this.AddItemsToContainerAsync(this.containerWithIndex);
        }
        private async Task AddItemsToContainerAsync(Container container)
        {
            await SingleWrite(container);
            await InsertBulkRecords(container);

        }
        private async Task SingleWrite(Container container)
        {
            this.isSingleRecord = true;
            await this.AddRecordsToDB("TMD002.TM.Test", "CalculateMeNow", "TMD002.TM.Test:MessageServer1", "mq://hostserver/interactiveserver/request", container, 1);
        }
        private async Task InsertBulkRecords(Container container)
        {
            await this.AddRecordsToDB("TMD001.TM.Test", "CalculateMeNow", "TMD001.TM.Test:MessageServer", "mq://hostserver/interactiveserver/request", container, 250);
            await this.AddRecordsToDB("TMD001.TM.Test", "EmployeeCalculations", "TMD001.TM.Test:MessageServer", "mq://hostserver/calculationserver/request", container, 250);
            await this.AddRecordsToDB("TMD001.TM.Test", "Distribution", "TMD001.TM.Test:MessageServer", "mq://hostserver/laborviewagentserver/request", container, 250);
            await this.AddRecordsToDB("TMD001.TM.Test", "", "TMD001.TM.Test:MessageServer", "mq://hostserver/maintenanceserver/request", container, 250);

            await this.AddRecordsToDB("TMD002.TM.Test", "CalculateMeNow", "TMD002.TM.Test:MessageServer", "mq://hostserver/interactiveserver/request", container, 249);
            await this.AddRecordsToDB("TMD002.TM.Test", "EmployeeCalculations", "TMD002.TM.Test:MessageServer", "mq://hostserver/calculationserver/request", container, 250);
            await this.AddRecordsToDB("TMD002.TM.Test", "Distribution", "TMD002.TM.Test:MessageServer", "mq://hostserver/laborviewagentserver/request", container, 250);
            await this.AddRecordsToDB("TMD002.TM.Test", "", "TMD002.TM.Test:MessageServer", "mq://hostserver/maintenanceserver/request", container, 250);
        }
        private async Task AddRecordsToDB(string tenant, string queue, string type, string location, Container container, int totalRecords)
        {
            int EmployeeId = 345;//totalRecords = 250,
            for (var i = 0; i < totalRecords; i++)
            {
                double requestCharge = 0;
                int elapsedTimeInMS = 0;
                var tasks = new List<Task>();

                var guid = Guid.NewGuid().ToString();
                Message message = new Message
                {
                    MessageId = guid,
                    SessionId = guid,
                    TenantId = tenant,
                    InstanceId = "",
                    MessageType = type,
                    MessageQueue = queue,
                    Description = queue,
                    Location = location,
                    Timestamp = new DateTime().ToString(),
                    SendTime = null,
                    SubmittedTime = new DateTime().ToString(),
                    MessageState = "scheduled",
                    Properties = new Property
                    {
                        TaskId = guid,
                        TaskCategory = null,
                        EmployeeId = (EmployeeId).ToString(),
                        PayPeriodType = "current"
                    },
                    Ttl = -1
                };
                tasks.Add(this.AddBulkItemAsync(message, container, tenant));
                await Task.WhenAll(tasks);
                if (queue != "" && container.Id == "ContainerWithDefaultIndex")
                {
                    MessageTrackingIndex msgTrackingIndex = new MessageTrackingIndex
                    {
                        id = guid,
                        //TMD001.TM.Test|message-server|class_name|EmployeeCalculations:345,current
                        messageIdentifier = tenant + "|message-server|class_name|" + queue + ":" + (EmployeeId++).ToString() + ",current",
                        TenantId = tenant
                    };
                    await this.AddBulkItemAsync(msgTrackingIndex, messageTrackingIndex, tenant);
                }


                if (i > 0 && i % 100 == 0)
                {
                    Console.WriteLine("{0} Items inserted to DB with Queue: {1}", i, queue);

                }
            }
        }
        private async Task AddBulkItemAsync(Message message, Container container, string partitionKey)
        {
            recordCount++;
            ItemResponse<Message> messageResponse;
            try
            {
                // Read the item to see if it exists.  
                messageResponse = await container.ReadItemAsync<Message>(message.MessageId, new PartitionKey(partitionKey));
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                messageResponse = await container.CreateItemAsync<Message>(message, new PartitionKey(partitionKey));
                if (this.isSingleRecord)
                {
                    Console.WriteLine("Total time elapsed for single write in MS: {0} ms Operation consumed {1} RUs.\n", messageResponse.Diagnostics.GetClientElapsedTime().TotalMilliseconds, messageResponse.RequestCharge);
                    this.isSingleRecord = false;
                }
            }
            timeTakenInMs += messageResponse.Diagnostics.GetClientElapsedTime().TotalMilliseconds;
            requestCharges += messageResponse.RequestCharge;

            if (recordCount % 1000 == 0)
            {
                Console.WriteLine("Time taken to write 1000 records to container{0} in: {1} ms, RU's {2}: ", container.Id, timeTakenInMs, requestCharges);
                timeTakenInMs = 0;
                requestCharges = 0;
            }
        }
        private async Task AddBulkItemAsync(MessageTrackingIndex message, Container container, string partitionKey)
        {
            ItemResponse<MessageTrackingIndex> messageResponse;
            try
            {
                // Read the item to see if it exists.  
                messageResponse = await container.ReadItemAsync<MessageTrackingIndex>(message.id, new PartitionKey(partitionKey));
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                messageResponse = await container.CreateItemAsync<MessageTrackingIndex>(message, new PartitionKey(partitionKey));
               
            }
        }
        private async Task QueryItemsAsync()
        {

            //single Read

            Console.WriteLine("\n----------------------Single Read: Query by MessageType with Default indexing|Partition Key-TenantID------------------------");
            await QueryByMessageType("TMD002.TM.Test:MessageServer1", this.containerWithDefaultIndex, "TMD002.TM.Test");
            Console.WriteLine("\n----------------------Single Read: Query by MessageType with indexing|Partition Key-TenantID---------------------------");
            await QueryByMessageType("TMD002.TM.Test:MessageServer1", this.containerWithIndex, "TMD002.TM.Test");

            //Bulk Read
            Console.WriteLine("\n----------------------Query by messageID with Default indexing|Partition Key-TenantID------------------------");
            await QueryByMessageId("16f0eaea-db8b-4b74-be5e-14b0b4286cce", this.containerWithDefaultIndex);
            Console.WriteLine("\n----------------------Query by messageID with indexing|Partition Key-TenantID---------------------------");
            await QueryByMessageId("16f0eaea-db8b-4b74-be5e-14b0b4286cce", this.containerWithIndex);

            Console.WriteLine("\n----------------------Bulk Read: Query by MessageType with Default indexing|Partition Key-TenantID------------------------");
            await QueryByMessageType("TMD001.TM.Test:MessageServer", this.containerWithDefaultIndex);
            Console.WriteLine("\n----------------------Bulk Read: Query by MessageType with indexing|Partition Key-TenantID---------------------------");
            await QueryByMessageType("TMD001.TM.Test:MessageServer", this.containerWithIndex);


            Console.WriteLine("\n----------------------Query by Message Queue with Default indexing|Partition Key-TenantID------------------------");
            await QueryByMessageQueue("CalculateMeNow", this.containerWithDefaultIndex);
            Console.WriteLine("\n----------------------Query by Message Queue with indexing|Partition Key-TenantID---------------------------");
            await QueryByMessageQueue("CalculateMeNow", this.containerWithIndex);

            Console.WriteLine("\n----------------------Query by Message Properties without indexing|Partition Key-TenantID------------------------");
            await QueryByMessageProperties("345", this.containerWithDefaultIndex);
            Console.WriteLine("\n----------------------Query by Message Properties with indexing|Partition Key-TenantID---------------------------");
            await QueryByMessageProperties("345", this.containerWithIndex);


            Console.WriteLine("\n----------------------Query for duplicate message|Partition Key-TenantID------------------------");
            await QueryForDuplicateMessage("TMD001.TM.Test|message-server|class_name|EmployeeCalculations:345,current", messageTrackingIndex);

        }
        private async Task QueryByMessageId(string messageId, Container container)
        {
            var sqlQueryText1 = "\nSELECT * FROM c WHERE c.id ='" + messageId + "'";

            Console.WriteLine("\n Running query: {0}\n", sqlQueryText1);

            await this.ReadItemAsync(sqlQueryText1, container);
        }
        private async Task QueryByMessageType(string messageType, Container container, string partitionKey = "TMD001.TM.Test")
        {
            //Query by Type
            var sqlQueryText = "SELECT * FROM c WHERE c.messageType = '" + messageType + "'";

            Console.WriteLine("\nRunning query: {0}\n", sqlQueryText);

            await this.ReadItemAsync(sqlQueryText, container, partitionKey);

        }
        private async Task QueryByMessageQueue(string messageQueue, Container container)
        {
            //Query by Queue
            var sqlQueryText = "SELECT * FROM c WHERE c.messageQueue ='" + messageQueue + "'";

            Console.WriteLine("\nRunning query: {0}\n", sqlQueryText);

            await this.ReadItemAsync(sqlQueryText, container);

        }
        private async Task QueryByMessageProperties(string empId, Container container)
        {
            //Query by Properties
            var sqlQueryText = "SELECT * FROM c WHERE c.properties.employeeId = '" + empId + "'";

            Console.WriteLine("Running query: {0}\n", sqlQueryText);

            await this.ReadItemAsync(sqlQueryText, container);

        }
        private async Task ReadItemAsync(string sqlQueryText, Container container, string partitionKey = "TMD001.TM.Test")
        {
            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);
            QueryRequestOptions queryRequestOptions = new QueryRequestOptions();
            queryRequestOptions.PartitionKey = new PartitionKey(partitionKey);
            queryRequestOptions.MaxConcurrency = -1;
            queryRequestOptions.MaxBufferedItemCount = -1;
            FeedIterator<Message> queryResultSetIterator = container.GetItemQueryIterator<Message>(queryDefinition, null, queryRequestOptions);

            double totalMS = 0;
            double RequestCharge = 0;
            double resultCount = 0;

            while (queryResultSetIterator.HasMoreResults)
            {
                FeedResponse<Message> currentResultSet = await queryResultSetIterator.ReadNextAsync();

                totalMS += currentResultSet.Diagnostics.GetClientElapsedTime().TotalMilliseconds;
                RequestCharge += currentResultSet.RequestCharge;
                resultCount += currentResultSet.Count;
            }
            Console.WriteLine("Total Request Charge=" + RequestCharge + " RUs");
            Console.WriteLine("Total Result count= " + resultCount);
            Console.WriteLine("Total Elapsed Time in Milliseconds= " + totalMS);
        }
        private async Task QueryForDuplicateMessage(string messageIdentifier, Container container)
        {
            //Query by Properties
            var sqlQueryText = "SELECT * FROM c WHERE c.messageIdentifier = '" + messageIdentifier + "'";

            Console.WriteLine("Running query: {0}\n", sqlQueryText);

            await this.ReadItemAsync(sqlQueryText, container);

        }


        private async Task QueryByStoredProcedureAsync()
        {
            var scriptId = "sp_getmessageType12";
            var param = "TMD001.TM.Test:MessageServer";
            var tenantId = "TMD002.TM.Test";
            double totalMS = 0;
            double RequestCharge = 0;
            double resultCount = 0;
            StoredProcedureResponse sproc;
            try
            {
                // Read the item to see if it exists.  
                sproc = await this.containerWithDefaultIndex.Scripts.ReadStoredProcedureAsync(scriptId);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                sproc = await this.containerWithDefaultIndex.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties(scriptId, File.ReadAllText(@"../../sp_getmessagetype.js")));
                Console.WriteLine($"\r\nCreated Store procedure Id:{sproc.Resource.Id} ");

            }
            
             
            StoredProcedureExecuteResponse<String> response = await this.containerWithDefaultIndex.Scripts.ExecuteStoredProcedureAsync<String>(
                scriptId,
                new PartitionKey("TMD001.TM.Test"),
                new dynamic[] { tenantId ,param}, 
                new StoredProcedureRequestOptions { EnableScriptLogging = true });
            dynamic jsonResponse = JsonConvert.DeserializeObject(response.Resource);
            Console.WriteLine(jsonResponse.ToObject<List<Message>>());
            //FeedIterator<Message> queryResultSetIterator = this.containerWithDefaultIndex.GetItemQueryIterator<Message>();
            //while (queryResultSetIterator.HasMoreResults)
            //{
            //    FeedResponse<Message> currentResultSet = await queryResultSetIterator.ReadNextAsync();
            //    //numDocs += response.Count();
            //    totalMS += currentResultSet.Diagnostics.GetClientElapsedTime().TotalMilliseconds;
            //    RequestCharge += currentResultSet.RequestCharge;
            //    resultCount += currentResultSet.Count;
            //}
            Console.WriteLine("Total Request Charge=" + response.RequestCharge + " RUs");
            Console.WriteLine("Total Result count= " + response.Resource.Length);
            Console.WriteLine("Total Elapsed Time in Milliseconds= " + response.Diagnostics.GetClientElapsedTime().TotalMilliseconds);
        }
           

        #endregion

    }
}
