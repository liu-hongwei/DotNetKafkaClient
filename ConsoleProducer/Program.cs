using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using DotNetKafkaClient;
using log4net.Config;
using ThreadState = System.Threading.ThreadState;

namespace ProducerConsole
{
    public class Program
    {
        static void Main(string[] args)
        {
            var _topic = ConfigurationManager.AppSettings.Get("topic");
            var _bks = ConfigurationManager.AppSettings.Get("brokers");
            var _producer = ConfigurationManager.AppSettings.Get("producerid");

            var log4netSection = ConfigurationManager.GetSection("log4net");
            if (log4netSection != null)
            {
                XmlConfigurator.Configure();
            }

            log4net.ILog logger = log4net.LogManager.GetLogger(typeof(Program).Name);

            Console.Title = "Prodcuer - " + "{ topic : " + _topic + " }";

            var brokers = new Dictionary<int, Broker>();
            int id = 1;

            var parts = _bks.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
            foreach (var part in parts)
            {
                var pairs = part.Split(new char[] { ':' }, StringSplitOptions.RemoveEmptyEntries);
                if (pairs.Length == 2)
                {
                    var ip = pairs[0];
                    var port = pairs[1];

                    if (!brokers.ContainsKey(id))
                    {
                        brokers.Add(id, new Broker(id, ip, int.Parse(port)));
                        id++;
                    }
                }
            }

            var config = new KafkaConfiguration()
            {
                ConsumerId = _producer,
                KafkaBrokers = brokers.Select(x => new IPEndPoint(IPAddress.Parse(x.Value.Host), x.Value.Port)).ToArray()
            };

            KafkaProducer producer = new KafkaProducer(config);
           

            if (producer == null)
            {
                throw  new Exception("[user] => fail to create producer");
            }

            DateTime startUtc = DateTime.UtcNow;
            double total = 0.0;
            while (true)
            {
                try
                {
                    var text = string.Format("{{\"time\":\"{0}\",\"id\":\"{1}\"}}", DateTime.UtcNow,
                        Guid.NewGuid().ToString("N"));
                    var bytes = Encoding.UTF8.GetBytes(text);

                    producer.SendMessage(_topic, -1, string.Empty, text);

                    total++;
                    var seconds = TimeSpan.FromTicks(DateTime.UtcNow.Ticks - startUtc.Ticks).TotalSeconds;
                    var speed = seconds == 0 ? 0 : total/seconds;

                    logger.Debug("[user] => producer average speed " + speed + " / seconds, total " + total + ", seconds " + seconds);
                }
                catch (Exception e)
                {
                    logger.Debug("[user] => fail to produce data \r\n" + e.Message);
                }
            }

            logger.Debug("[user] => Program is exiting, press any key to exit");
            Console.ReadKey();
        }
    }
}
