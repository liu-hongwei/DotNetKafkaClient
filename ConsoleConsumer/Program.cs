using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using DotNetKafkaClient;
using log4net.Config;

namespace ConsumerConsole
{
    public class Program
    {
        static void Main(string[] args)
        {
            var log4netSection = ConfigurationManager.GetSection("log4net");
            if (log4netSection != null)
            {
                XmlConfigurator.Configure();
            }

            log4net.ILog logger = log4net.LogManager.GetLogger(typeof(Program).Name);

            var _topic = ConfigurationManager.AppSettings.Get("topic");
            var _bks = ConfigurationManager.AppSettings.Get("brokers");
            var _groupid = ConfigurationManager.AppSettings.Get("consumergroup");
            var _consumerid = ConfigurationManager.AppSettings.Get("consumerid");
            var _mode = ConfigurationManager.AppSettings.Get("consumermode");

            bool balanceMode = string.Compare(_mode, "group", StringComparison.OrdinalIgnoreCase) == 0;
            var _autocommit = false;
            bool.TryParse(ConfigurationManager.AppSettings.Get("autocommit"), out _autocommit);

            var _userconnector = false;
            bool.TryParse(ConfigurationManager.AppSettings.Get("userconsumerconnector"), out _userconnector);

            var _useasyncsocket = false;
            bool.TryParse(ConfigurationManager.AppSettings.Get("asyncsocket"), out _useasyncsocket);

            var brokers = new Dictionary<int, Broker>();
            int id = 1;

            var parts = _bks.Split(new char[] {','}, StringSplitOptions.RemoveEmptyEntries);
            foreach (var part in parts)
            {
                var pairs = part.Split(new char[] {':'}, StringSplitOptions.RemoveEmptyEntries);
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
                BalanceInGroupMode = balanceMode,
                AutoOffsetReset = OffsetRequest.SmallestTime,
                AutoCommit = _autocommit,
                GroupId = _groupid,
                ConsumerId = _consumerid,
                KafkaBrokers = brokers.Select(x => new IPEndPoint(IPAddress.Parse(x.Value.Host), x.Value.Port) ).ToArray()
            };

            Console.Title = "ConsumerConsole - {" + " group : " + _groupid + " , consumer : " + _consumerid + " , topic : " + _topic + " }";

            var kfConsumer = new KafkaConsumer(config);
            kfConsumer.Subscribe(new string[] { _topic });

            if (!balanceMode)
            {
                ThreadParameter parameter = new ThreadParameter();
                parameter.Topic = _topic;
                parameter.Consumer = kfConsumer;
                parameter.Logger = logger;

                var thread = new Thread(OffsetCheckThread);
                thread.Start(parameter);
            }

            double handledMessageCounts = 0;
            DateTime startUtc = DateTime.UtcNow;

            StringBuilder sb = new StringBuilder();
            while (true)
            {
                try
                {
                    var messages = kfConsumer.ReadMessages().ToList();

                    handledMessageCounts += messages.Count;
                    double seconds = TimeSpan.FromTicks(DateTime.UtcNow.Ticks - startUtc.Ticks).TotalSeconds;
                    double speed = seconds == 0 ? 0 : handledMessageCounts / seconds;
                    logger.Debug("data process speed " + speed + " total processed messages " + handledMessageCounts + " total seconds " + seconds + " begin at " + startUtc);
                    /*
                    foreach (Message message in messages)
                    {
                        string text = string.Empty;
                        const int MAX_SIZE = 2048;
                        if (message.Payload.Length <= MAX_SIZE)
                        {
                            text = Encoding.UTF8.GetString(message.Payload);
                            sb.Append(text);
                        }
                        else
                        {
                            int start = 0;
                            int end = message.Payload.Length;
                            int count = MAX_SIZE;
                            while (start < end)
                            {
                                if (end - start < MAX_SIZE)
                                {
                                    count = end - start;
                                }

                                var nChars = Encoding.UTF8.GetCharCount(message.Payload, start, count);
                                char[] chars = new char[nChars];
                                nChars = Encoding.UTF8.GetChars(message.Payload, start, count, chars, 0);
                                sb.Append(new string(chars, 0, nChars));
                                start += count;
                            }
                        }

                        logger.Debug(sb.ToString());
                        sb.Clear();
                    }
                    */
                    messages.Clear();
                }
                catch (Exception e)
                {
                    logger.Debug("[user] => unhandled exception : " + e.Message + " " + e.StackTrace);
                }
            }


            logger.Debug("[user] => Program is exiting, press any key to exit");
            Console.ReadKey();

        }

        class ThreadParameter
        {
            public KafkaConsumer Consumer;
            public string Topic;
            public log4net.ILog Logger;
        }
        static void OffsetCheckThread(object input)
        {
            ThreadParameter parameter = (ThreadParameter)input;
            KafkaConsumer consumer = parameter.Consumer;
            string topic = parameter.Topic;
            log4net.ILog logger = parameter.Logger;

            while (true)
            {
                try
                {
                    // read commited offset and producer offset
                    var committedOffset = consumer.GetLastCommittedOffset(topic);
                    var latestOffset = consumer.GetLatestDataOffset(topic);

                    // display data
                    var display = new StringBuilder();
                    display.AppendLine(string.Format("==================begin consumer info============================"));
                    display.AppendLine(string.Format("{0, -15} {1,-15} {2,-15} {3,-15}", "partitionId", "commitedoffset", "dataoffset", "lag"));
                    foreach (var committed in committedOffset)
                    {
                        if (latestOffset.ContainsKey(committed.Key))
                        {
                            var latest = latestOffset[committed.Key];
                            var lag = latest - committed.Value;
                            display.AppendLine(string.Format("{0, -15} {1,-15} {2,-15} {3,-15}", committed.Key, committed.Value, latest, lag < 0 ? "commited offset (" + committed.Value.ToString() + ") > latest offset (" + latest.ToString() + ")" : lag.ToString()
                                ));
                        }
                        else
                        {
                            display.AppendLine(string.Format("{0, -15} {1,-15} {2,-15} {3,-15}", committed.Key, committed.Value, "unknown", "unknown"));
                        }
                    }
                    display.AppendLine(string.Format("==================end  consumer  info============================"));
                    logger.Debug(display.ToString());

                }
                catch (Exception e)
                {
                    logger.Debug("[user] => list consumer info failed\r\n" + e.Message);
                }

                Thread.Sleep(5 * 1000);
            }
        }
    }
}