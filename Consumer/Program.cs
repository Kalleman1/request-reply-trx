using System.Text;
using System;
using System.Text;
using RabbitMQ.Client;
using Newtonsoft.Json;
using Shared.Models;
using RabbitMQ.Client.Events;

namespace Consumer
{
    class Program
    {
        // Beklager det ikke overholder SOLID principperne.
        // Derudover kunne jeg have oprettet nogle klasser til at holde 
        // Styr på de forskellige metoder, hvis jeg havde lidt mere tid.

        // Opretter et nyt random objekt, som senere bliver brugt til at
        // Simulere en fejl i løbet af processeringen af beskeden. (Database operation)
        private static readonly Random _random = new Random();
        public static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                // Opret exchange og queue, og bind dem
                channel.ExchangeDeclare(exchange: "request_exchange", type: ExchangeType.Direct);
                channel.QueueDeclare(queue: "request_queue", durable: true, exclusive: false, autoDelete: false, arguments: null);
                channel.ExchangeDeclare(exchange: "reply_exchange", type: ExchangeType.Direct);
                channel.QueueBind(queue: "request_queue", exchange: "request_exchange", routingKey: "request_key");

                Console.WriteLine("Waiting for messages...");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    // Deserialize request message
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    var request = JsonConvert.DeserializeObject<RequestMessage>(message);

                    Console.WriteLine($"Received request with WorkItemId: {request.WorkItemId}");

                    // Start transaction
                    channel.TxSelect();

                    try
                    {
                        // Simulér en fejl i database operationen
                        // 50% chance :P
                        if (_random.Next(0, 2) == 0)
                        {
                            throw new Exception("Simulated processing error");
                        }

                        // Simulér successfuld database operation
                        DatabaseOperation();

                        var reply = new ReplyMessage
                        {
                            WorkItemId = request.WorkItemId,
                            Success = true,
                            Message = "Task completed successfully"
                        };

                        // Encode reply message til json
                        var replyMessageBody = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(reply));
                        var replyProperties = channel.CreateBasicProperties();
                        replyProperties.CorrelationId = ea.BasicProperties.CorrelationId;

                        channel.BasicPublish(
                            exchange: "reply_exchange",
                            routingKey: "reply_key",
                            basicProperties: replyProperties,
                            body: replyMessageBody);

                        Console.WriteLine($"Sent reply for WorkItemId: {request.WorkItemId}");

                        // Kun commit transactionen hvis beskeden er sendt, og
                        // database operationen kører igennem uden fejl
                        channel.TxCommit();
                        Console.WriteLine("Transaction committed.");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error processing WorkItemId {request.WorkItemId}: {ex.Message}");

                        // Rollback transaction hvis der opstår en exception
                        channel.TxRollback();
                        RevertDatabaseOperation();
                    }
                };

                channel.BasicConsume(queue: "request_queue", autoAck: true, consumer: consumer);

                Console.WriteLine("Press [enter] to exit.");
                Console.ReadLine();
            }
        }

        public static void DatabaseOperation()
        {
            // Logik til at håndtere database ændring
            Console.WriteLine("Database operation completed.");
        }

        public static void RevertDatabaseOperation()
        {
            // Logik til at håndtere en revert af database ændring
            // Hvis der sker en fejl i løbet af message håndteringen
            Console.WriteLine("Database operation reverted.");
        }
    }
}