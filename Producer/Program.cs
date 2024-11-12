

using System.Text;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Shared.Models;

namespace Consumer
{
    class Program
    {
        // Beklager det ikke overholder SOLID principperne.
        // Derudover kunne jeg have oprettet nogle klasser til at holde 
        // Styr på de forskellige metoder, hvis jeg havde lidt mere tid.
        public static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {   
                // Opret exchanges og queues og bind dem
                channel.ExchangeDeclare(exchange: "request_exchange", type: ExchangeType.Direct);
                channel.ExchangeDeclare(exchange: "reply_exchange", type: ExchangeType.Direct);
                channel.QueueDeclare(queue: "request_queue", durable: true, exclusive: false, autoDelete: false, arguments: null);
                channel.QueueBind(queue: "request_queue", exchange: "request_exchange", routingKey: "request_key");

                channel.QueueDeclare(queue: "reply_queue", durable: true, exclusive: false, autoDelete: false, arguments: null);
                channel.QueueBind(queue: "reply_queue", exchange: "reply_exchange", routingKey: "reply_key");

                // Starter transacation
                channel.TxSelect();

                // Opretter et work item objekt med status Pending
                var request = new RequestMessage
                {
                    WorkItemId = Guid.NewGuid().ToString(),
                    TaskDetails = "Process this task",
                    State = WorkItemState.Pending
                };

                // Serialize Work item objektet til Json
                var messageBody = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(request));
                var properties = channel.CreateBasicProperties();
                properties.CorrelationId = request.WorkItemId;
                properties.ReplyTo = "reply_queue";

                // Send beskeden til Request Exchange
                channel.BasicPublish(
                    exchange: "request_exchange",
                    routingKey: "request_key",
                    basicProperties: properties,
                    body: messageBody);

                Console.WriteLine($"Sent request with WorkItemId: {request.WorkItemId}");

                // Kun commit transaction HVIS beskeden er sendt afsted.
                channel.TxCommit();
                request.State = WorkItemState.Committed;
                Console.WriteLine("Transaction committed.");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    // Tjek at CorrelationId matcher WorkItemId
                    if (ea.BasicProperties.CorrelationId == request.WorkItemId)
                    {
                        var replyBody = ea.Body.ToArray();
                        var replyMessage = Encoding.UTF8.GetString(replyBody);
                        var reply = JsonConvert.DeserializeObject<ReplyMessage>(replyMessage);

                        Console.WriteLine($"Received reply for WorkItemId {reply.WorkItemId}: {reply.Message}");
                    }
                };

                // Consume messages fra reply_queue
                channel.BasicConsume(queue: "reply_queue", autoAck: true, consumer: consumer);

                Console.WriteLine("Press [enter] to exit.");
                Console.ReadLine();
            }
        }
    }
}