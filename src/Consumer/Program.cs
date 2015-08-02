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
        static readonly object sync = new object();
        static readonly List<ReceivedMessage> receivedItems = new List<ReceivedMessage>();
        
        public void Main(string[] args)
        {
            var options = GetOptions(args);
            if (options == null) return;

            StartReporting();

            var kafkaOptions = new KafkaOptions(options.KafkaNodeUri);
            using (var router = new BrokerRouter(kafkaOptions))
            using (var client = new KafkaNet.Consumer(new ConsumerOptions("TestHarness", router) { Log = new ConsoleLog(), MinimumBytes = 1 }))
            {
                Console.WriteLine("Listening for messages...");
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
        
        static Options GetOptions(string[] args) {
            var options = new Options();
            if (args.Length == 0 || (args.Length == 1 && args[0] == "help")) {
                WriteHelp();
                return null;
            }
            
            if (args.Length >= 1) {
                try {
                    options.KafkaNodeUri = new Uri(args[0]);                    
                } catch (Exception ex) {
                    Console.Error.WriteLine(ex.ToString());
                    Console.Error.WriteLine("Hmm... you need to give me a valid URI for a Kafka node... Maybe try asking me for help.");
                    return null;
                }
            }
            
            return options;
        }
        
        static void StartReporting() {
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
        }

        private class ReceivedMessage
        {
            public DateTime DateTime { get; set; }
            public double TotalMilliseconds { get; set; }
        }
        
        static void WriteHelp()
        {
            Console.WriteLine("Hello. I'm the Kafka Producer. I... produce messages.");
            Console.WriteLine("Just tell me where kafka is and how long to sleep between each message.");
            Console.WriteLine("\tE.g. \"dnx . Producer http://192.168.59.103:9092 100\" will publish one message to a kafka install at 192.168.59.103 on port 9092 every 100ms");
            Console.WriteLine("If you don't tell me where kafka is we're going to have a bit of a problem...");
            Console.WriteLine("If you don't tell me how long to wait I just won't wait at all! :-)");
        }
    }
    
    class Options {
        public Uri KafkaNodeUri { get; set; }
        public int SleepDuration { get; set; }
    }
}
