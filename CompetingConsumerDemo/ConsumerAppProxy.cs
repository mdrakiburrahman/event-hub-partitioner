using System.Diagnostics;

namespace CompetingConsumersEventHub
{
    class ConsumerAppProxy
    {
        /// <summary>
        /// Starts the consumer app process.
        /// </summary>
        /// <param name="eventhubConnectionString">The Event Hub connection string.</param>
        /// <param name="blobConnectionString">The Blob connection string.</param>
        /// <param name="queueName">The queue name.</param>
        /// <param name="consumerGroup">The consumer group.</param>
        /// <param name="id">The ID.</param>
        /// <returns>The <see cref="IDisposable"/> instance representing the process killer.</returns>
        public static IDisposable StartConsumerApp(
            string eventhubConnectionString,
            string blobConnectionString,
            string queueName,
            object consumerGroup,
            int id
        )
        {
            ProcessStartInfo psi = new ProcessStartInfo(
                "ConsumerApp.exe",
                string.Join(' ', eventhubConnectionString, blobConnectionString, queueName, consumerGroup, id.ToString())
            )
            {
                CreateNoWindow = false,
                WindowStyle = ProcessWindowStyle.Normal,
                UseShellExecute = true,
            };
            var process = Process.Start(psi);

            return new ProcessKiller(process);
        }

        /// <summary>
        /// Represents a class that kills the given process.
        /// </summary>
        class ProcessKiller : IDisposable
        {
            private Process p;

            public ProcessKiller(Process p)
            {
                this.p = p;
            }

            public void Dispose()
            {
                p.Kill(true);
            }
        }
    }
}
