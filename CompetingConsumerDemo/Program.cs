using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;

namespace CompetingConsumersEventHub
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Starting Azure Event Hubs Demo");

            var eventhubConnectionString = Environment.GetEnvironmentVariable("EH_CONN_STRING");
            var blobConnectionString = Environment.GetEnvironmentVariable("BLOB_CONN_STRING");

            if (string.IsNullOrEmpty(eventhubConnectionString))
                throw new Exception("Env var EH_CONN_STRING is null or empty");
            if (string.IsNullOrEmpty(blobConnectionString))
                throw new Exception("Env var BLOB_CONN_STRING is null or empty");

            var eventHubName = "demoeventhubwith4partitions";
            var consumerGroup = "$Default";

            EventHubProducerClient client = new EventHubProducerClient(
                eventhubConnectionString,
                eventHubName
            );

            var consumerId = 1;

            List<IDisposable> d = new List<IDisposable>();
            d.Add(
                ConsumerAppProxy.StartConsumerApp(
                    eventhubConnectionString,
                    blobConnectionString,
                    eventHubName,
                    consumerGroup,
                    consumerId++
                )
            );

            // Send a message every 500ms
            //
            ThreadPool.QueueUserWorkItem(
                async (state) =>
                {
                    int messsageNumber = 0;

                    Random r = new Random();

                    while (true)
                    {
                        var messageText =
                            $"Sending message number {messsageNumber} at {DateTime.UtcNow.TimeOfDay}";

                        var ourMessage = System.Text.Encoding.UTF8.GetBytes(messageText);
                        var msg = new EventData(ourMessage);
                        await client.SendAsync(
                            new EventData[] { msg },
                            // Partitioning strategy
                            //
                            // <param name="partitionId">The identifier of the partition to which events should be sent.</param>                             <-- Deterministic
                            // <param name="partitionKey">The hashing key to use for influencing the partition to which the events are routed.</param>       <-- Hash based
                            //
                            new SendEventOptions { PartitionKey = messsageNumber.ToString() }
                        );
                        await Task.Delay(r.Next(500));
                        messsageNumber++;
                    }
                }
            );

            while (true)
            {
                var messageText = Console.ReadLine();

                if (messageText == "quit")
                    break;

                if (messageText == "up")
                {
                    d.Add(
                        ConsumerAppProxy.StartConsumerApp(
                            eventhubConnectionString,
                            blobConnectionString,
                            eventHubName,
                            consumerGroup,
                            consumerId++
                        )
                    );
                    continue;
                }

                if (messageText == "down")
                {
                    var oneToKill = d[d.Count - 1];
                    d.RemoveAt(d.Count - 1);
                    oneToKill.Dispose();
                    continue;
                }

                foreach (var bit in messageText.Split(' '))
                {
                    var ourMessage = System.Text.Encoding.UTF8.GetBytes(bit);
                    var msg = new EventData(ourMessage);
                    await client.SendAsync(
                        new EventData[] { msg },
                        new SendEventOptions { PartitionKey = messageText }
                    );
                }
            }
        }
    }
}
