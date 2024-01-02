using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;

namespace CompetingConsumersEventHub
{
    class EventHubReceiver
    {
        readonly string eventHubName;
        private readonly string consumerGroup;
        readonly string eventhubConnectionString;
        readonly string blobConnectionString;
        string id;

        public EventHubReceiver(
            string eventhubConnectionString,
            string blobConnectionString,
            string eventHubName,
            string consumerGroup,
            string id
        )
        {
            this.eventhubConnectionString =
                eventhubConnectionString
                ?? throw new Exception("eventhubConnectionString cannot be null");
            this.blobConnectionString =
                blobConnectionString ?? throw new Exception("blobConnectionString cannot be null");
            this.id = id ?? throw new Exception("Id cannot be null");
            this.eventHubName = eventHubName ?? throw new Exception("EventHubName cannot be null");
            this.consumerGroup =
                consumerGroup ?? throw new Exception("ConsumerGroup cannot be null");
        }

        public async Task Go()
        {
            Console.WriteLine($"Starting our Event Hub Receiver {id}");
            string containerName = "offsetcontainer";

            BlobContainerClient blobContainerClient = new BlobContainerClient(
                blobConnectionString,
                containerName
            );

            EventProcessorClient processor = new EventProcessorClient(
                blobContainerClient,
                "$Default",
                eventhubConnectionString,
                eventHubName
            );

            processor.ProcessEventAsync += Processor_ProcessEventAsync;
            processor.ProcessErrorAsync += Processor_ProcessErrorAsync;

            await processor.StartProcessingAsync();
            Console.WriteLine("Started the processor");

            Console.ReadLine();
            await processor.StopProcessingAsync();
            Console.WriteLine("Stopped the processor");
        }

        private static Task Processor_ProcessErrorAsync(ProcessErrorEventArgs arg)
        {
            Console.WriteLine("Error Received: " + arg.Exception.ToString());
            return Task.CompletedTask;
        }

        private static async Task Processor_ProcessEventAsync(ProcessEventArgs arg)
        {
            Console.WriteLine(
                $"Event Received from Partition {arg.Partition.PartitionId}: {arg.Data.EventBody.ToString()}"
            );

            await arg.UpdateCheckpointAsync();
        }
    }
}
