using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs;
using Azure.Storage.Blobs;
using Azure.Messaging.EventHubs.Processor;
using Azure.Messaging.EventHubs.Producer;
using Newtonsoft.Json.Linq;
using System.Text;
using System.Text.Json.Serialization;
using Newtonsoft.Json;

namespace ArnPartitionReplicator
{
    public class Program
    {
        public const string sourceEventHubName = "armlinkednotifications";
        public const string destinationSinglePartitionEventHubName = "arnsinglepartition";
        public const string destinationManyPartitionEventHubName = "arn32partition";

        static async Task Main(string[] args)
        {
            var sourceEventhubConnectionString = Environment.GetEnvironmentVariable(
                "SRC_EH_CONN_STRING"
            );
            var destinationeEventhubConnectionString = Environment.GetEnvironmentVariable(
                "DEST_EH_CONN_STRING"
            );
            var blobConnectionString = Environment.GetEnvironmentVariable(
                "OFFSET_BLOB_CONN_STRING"
            );

            // Create a cancellation token source to handle termination signals
            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (sender, eventArgs) =>
            {
                eventArgs.Cancel = true; // Cancel the default termination behavior
                cts.Cancel(); // Cancel the ongoing operation
            };

            // Create a blob container client that the event processor will use
            BlobContainerClient storageClient = new BlobContainerClient(
                connectionString: blobConnectionString,
                blobContainerName: "offsetcontainer"
            );

            // Initiate Processor Client
            var processorClient = new EventProcessorClient(
                checkpointStore: storageClient,
                consumerGroup: EventHubConsumerClient.DefaultConsumerGroupName,
                connectionString: sourceEventhubConnectionString,
                eventHubName: sourceEventHubName
            );

            // Initiate Consumer Client
            var consumerClient = new EventHubConsumerClient(
                consumerGroup: EventHubConsumerClient.DefaultConsumerGroupName,
                connectionString: sourceEventhubConnectionString,
                eventHubName: sourceEventHubName
            );

            // Single partition client
            EventHubProducerClient singlePartitionDestinationClient = new EventHubProducerClient(
                destinationeEventhubConnectionString,
                destinationSinglePartitionEventHubName
            );

            // Many partition client
            EventHubProducerClient manyPartitionDestinationClient = new EventHubProducerClient(
                destinationeEventhubConnectionString,
                destinationManyPartitionEventHubName
            );

            // Register handlers for processing events and handling errors
            processorClient.ProcessEventAsync += async (eventArgs) =>
            {
                await ProcessEventHandler(
                    eventArgs,
                    consumerClient,
                    singlePartitionDestinationClient,
                    manyPartitionDestinationClient
                );
            };
            processorClient.ProcessErrorAsync += ProcessErrorHandler;

            // Start the processing
            Console.WriteLine(
                $"Starting the processor, there are currently {GetNumEvents(consumerClient).Result} events in the queue."
            );
            await processorClient.StartProcessingAsync();

            // Wait for cancellation signal
            Console.WriteLine("Processor started. Press Ctrl+C to stop.");

            try
            {
                await WaitForCancellationSignalAsync(cts.Token);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Hit Exception: {e}");
            }
            finally
            {
                await processorClient.StopProcessingAsync();
                await consumerClient.CloseAsync();
                Console.WriteLine("Exiting Processor.");
            }
        }

        static async Task<Task> ProcessEventHandler(
            ProcessEventArgs eventArgs,
            EventHubConsumerClient consumerClient,
            EventHubProducerClient singlePartitionDestinationClient,
            EventHubProducerClient manyPartitionDestinationClient
        )
        {
            string payload = Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray());
            List<ArnEventHubSchemaV3>? arnPayload;
            string subject = "";
            try
            {
                arnPayload = System.Text.Json.JsonSerializer.Deserialize<List<ArnEventHubSchemaV3>>(
                    payload
                );
                subject = arnPayload[0]?.Subject;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception occurred: {ex}");
            }

            // Send to single partition
            var replicatedMessage = new EventData(System.Text.Encoding.UTF8.GetBytes(payload));
            await singlePartitionDestinationClient.SendAsync(
                new EventData[] { replicatedMessage },
                new SendEventOptions { PartitionKey = subject }
            );

            // Send to many partitions
            await manyPartitionDestinationClient.SendAsync(
                new EventData[] { replicatedMessage },
                new SendEventOptions { PartitionKey = subject }
            );

            // await eventArgs.UpdateCheckpointAsync(); // This takes too much time, we don't really care about dupe events, so don't update checkpoint per event.

            return Task.CompletedTask;
        }

        static Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
        {
            Console.WriteLine(
                $"\tPartition '{eventArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen."
            );
            Console.WriteLine(eventArgs.Exception.Message);
            return Task.CompletedTask;
        }

        static async Task<long> GetNumEvents(EventHubConsumerClient consumerClient)
        {
            var partitionProperties = await consumerClient.GetPartitionPropertiesAsync("0");
            return partitionProperties.LastEnqueuedSequenceNumber;
        }

        static async Task WaitForCancellationSignalAsync(CancellationToken cancellationToken)
        {
            // Wait indefinitely or until cancellation signal
            await Task.Delay(-1, cancellationToken);
        }

        public class ArnEventHubSchemaV3
        {
            [JsonPropertyName("id")]
            public string Id { get; set; }

            [JsonPropertyName("subject")]
            public string Subject { get; set; }

            [JsonPropertyName("data")]
            public object Data { get; set; }

            [JsonPropertyName("eventType")]
            public string EventType { get; set; }

            [JsonPropertyName("dataVersion")]
            public string DataVersion { get; set; }

            [JsonPropertyName("metadataVersion")]
            public string MetadataVersion { get; set; }

            [JsonPropertyName("eventTime")]
            public string EventTime { get; set; }

            [JsonPropertyName("topic")]
            public string Topic { get; set; }
        }
    }
}
