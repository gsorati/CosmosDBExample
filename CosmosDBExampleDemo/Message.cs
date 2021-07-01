using Newtonsoft.Json;

namespace CosmosDBExampleDemo
{
    public class Message
    {
        [JsonProperty(PropertyName = "id")]
        public string MessageId { get; set; }

        [JsonProperty(PropertyName = "sessionId")]
        public string SessionId { get; set; }

        
        public string TenantId { get; set; }

        [JsonProperty(PropertyName = "instanceId")]
        public string InstanceId { get; set; }

        [JsonProperty(PropertyName = "messageType")]
        public string MessageType { get; set; }

        [JsonProperty(PropertyName = "messageQueue")]
        public string MessageQueue { get; set; }

        [JsonProperty(PropertyName = "description")]
        public string Description { get; set; }

        [JsonProperty(PropertyName = "location")]
        public string Location { get; set; }

        [JsonProperty(PropertyName = "timestamp")]
        public string Timestamp { get; set; }

        [JsonProperty(PropertyName = "sendTime")]
        public string SendTime { get; set; }

        [JsonProperty(PropertyName = "submittedTime")]
        public string SubmittedTime { get; set; }

        [JsonProperty(PropertyName = "messageState")]
        public string MessageState { get; set; }

        [JsonProperty(PropertyName = "properties")]
        public Property Properties { get; set; }

        [JsonProperty(PropertyName = "ttl", NullValueHandling = NullValueHandling.Ignore)]
        public int? Ttl { get; set; }
    }

    public class Property
    {
        [JsonProperty(PropertyName = "taskId")]
        public string TaskId { get; set; }
        [JsonProperty(PropertyName = "taskCategory")]
        public string TaskCategory { get; set; }
        [JsonProperty(PropertyName = "employeeId")]
        public string EmployeeId { get; set; }
        [JsonProperty(PropertyName = "payPeriodType")]
        public string PayPeriodType { get; set; }
    }

    public class MessageTrackingIndex
    {
        public string id { get; set; }


        public string messageIdentifier { get; set; }


        public string TenantId { get; set; }
    }
}
