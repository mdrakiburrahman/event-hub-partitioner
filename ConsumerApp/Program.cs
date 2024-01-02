using CompetingConsumersEventHub;

namespace ConsumerApp
{
    public class Program
    {
        static async Task Main(string[] args)
        {
            var eventHubconnectionString = args[0];
            var blobHubconnectionString = args[1];
            var queueName = args[2];
            var consumerGroup = args[3];
            var id = args.Length > 4 ? args[4] : "";

            EventHubReceiver receiver1 = new EventHubReceiver(
                eventHubconnectionString,
                blobHubconnectionString,
                queueName,
                id,
                consumerGroup
            );
            await receiver1.Go();

            Console.ReadLine();
        }
    }
}
