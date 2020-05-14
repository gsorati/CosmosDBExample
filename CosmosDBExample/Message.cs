using Newtonsoft.Json;
namespace CosmosDBExample
{
    public class Message
    {
        [JsonProperty(PropertyName = "id")]
        public string MessageID { get; set; }
        public string MessageType { get; set; }
        public string Description { get; set; }
        public MessageInfo MessageInformation { get; set; }

        [JsonProperty(PropertyName = "ttl", NullValueHandling = NullValueHandling.Ignore)]
        public int? ttl { get; set; }
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }
    }

    public class MessageInfo {
        public string SendTime { get; set; }
        public string Location { get; set; }
    }
}
