
using Confluent.Kafka;
using System.Net;
using System.Text.Json;

string bootstrapServers = "localhost:9092";
string topic = "kafkatest02";
char c = ' ';
int id = 0;


while (c != 'q')
{
    var messagerequest = GetMessageRequest();
    string json = JsonSerializer.Serialize(messagerequest);
    await SendMessageRequest(topic, json);

    Console.WriteLine("Press any key to continue");
    Console.WriteLine("Press 'q' to exit");
    c = Console.ReadKey().KeyChar;
    Console.WriteLine("");
}




MessageRequest GetMessageRequest()
{
    string message = "";
    int color = -1;
    while (string.IsNullOrWhiteSpace(message) || color == -1)
    {
        Console.WriteLine("Write Your Message");
        message = Console.ReadLine();
        Console.WriteLine("Choose Color");

        for (int i = 0; i < 16; i++)
        {
            Console.ForegroundColor = (ConsoleColor)i;
            Console.WriteLine($"{i} . {((ConsoleColor)i).ToString()}");

        }
        Console.ResetColor();

        int colora;
        bool check = int.TryParse(Console.ReadLine(), out colora);

        if (!check)
            continue;
        color = colora;
    }
    id++;
    MessageRequest messageRequest = new() 
    { 
        Color = color, 
        Message = message,
        Id = id,
        UniqueId =  Guid.NewGuid(),
        StartProducer = DateTime.Now
    };

    return messageRequest;
}


async Task<bool> SendMessageRequest(string topic, string message)
{
    ProducerConfig config = new ProducerConfig
    {
        BootstrapServers = bootstrapServers,
        ClientId = Dns.GetHostName(),
        
    };

    try
    {
        TopicPartition topic1 = new TopicPartition(topic, new Partition());

        using (var producer = new ProducerBuilder
        <Null, string>(config).Build())
        {
            var result = await producer.ProduceAsync
            (topic1, new Message<Null, string>
            {
                Value = message
            });

            

            Console.WriteLine($"Delivery Timestamp:{result.Timestamp.UtcDateTime}");
            return await Task.FromResult(true);
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error occured: {ex.Message}");
    }

    return await Task.FromResult(false);
}