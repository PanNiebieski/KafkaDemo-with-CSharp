
using Confluent.Kafka;
using System.Net;
using System.Text.Json;

string topic = "kafkatest02";
string groupId = "kafkatest02_group";
string bootstrapServers = "localhost:9092";
int commitPeriod = 5;

var config = new ConsumerConfig
{
    GroupId = groupId,
    BootstrapServers = bootstrapServers,
    EnableAutoCommit = false,
    
    
    
};

try
{
    using (var consumerBuilder = new ConsumerBuilder
    <Ignore, string>(config).Build())
    {
        consumerBuilder.Subscribe(topic);
        var cancelToken = new CancellationTokenSource();

        try
        {
            while (true)
            {
                var consumer = consumerBuilder.Consume
                   (cancelToken.Token);
                var messageRequest = JsonSerializer.Deserialize
                    <MessageRequest>
                        (consumer.Message.Value);

                Console.WriteLine($"\nProcessing Number: {messageRequest.Id}");
                Console.WriteLine($"Processing UniqueId: {messageRequest.UniqueId}");
                Console.ForegroundColor = (ConsoleColor)messageRequest.Color;
                Console.WriteLine(messageRequest.Message);
                Console.ResetColor();
                Console.WriteLine("");


                if (consumer.Offset % commitPeriod == 0)
                {
                    try
                    {
                        Console.WriteLine("Commit");
                        consumerBuilder.Commit(consumer);
                    }
                    catch (KafkaException e)
                    {
                        Console.WriteLine($"Commit error: {e.Error.Reason}");
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            consumerBuilder.Close();
        }
    }
}
catch (Exception ex)
{
    Console.WriteLine(ex.Message);
}