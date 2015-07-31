using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Consumer
{
    public class Program
    {
        public void Main(string[] args)
        {
            Console.WriteLine("Press enter to start...");
            Console.ReadLine();
            var sync = new object();
            var receivedItems = new List<ReceivedMessage>();

            var task = Task.Run(() =>
            {
                var last = DateTime.MinValue;
                while (true)
                {
                    try
                    {
                        Thread.Sleep(1000);
                        ReceivedMessage[] items;
                        lock (sync)
                            items = receivedItems.Where(r => r.DateTime > last).ToArray();
                        if (items.Length == 0) continue;
                        Console.WriteLine($"Count: {items.Count()}msgs\tMax: {items.Max(r => r.TotalMilliseconds)}ms\tMin:{items.Min(r => r.TotalMilliseconds)}\tAve:{items.Average(r => r.TotalMilliseconds)}");
                        last = items.Max(r => r.DateTime);
                    }
                    catch (Exception ex)
                    {
                        Console.Error.WriteLine("Oops..." + ex);
                    }
                }
            });

            var options = new KafkaOptions(new Uri("http://localhost:9092"));
            using (var router = new BrokerRouter(options))
            using (var client = new KafkaNet.Consumer(new ConsumerOptions("TestHarness", new BrokerRouter(options)) { Log = new ConsoleLog(), MinimumBytes = 1 }))
            {
                foreach (var message in client.Consume())
                {
                    try
                    {
                        var received = DateTime.Now;
                        var sent = new DateTime(BitConverter.ToInt64(message.Value, 0));
                        var diff = received - sent;
                        lock (sync)
                            receivedItems.Add(new ReceivedMessage { DateTime = received, TotalMilliseconds = diff.TotalMilliseconds });
                    }
                    catch (Exception ex)
                    {
                        Console.Error.WriteLine("Oops... " + ex);
                    }
                }
            }
        }

        private class ReceivedMessage
        {
            public DateTime DateTime { get; set; }
            public double TotalMilliseconds { get; set; }
        }
    }
}
