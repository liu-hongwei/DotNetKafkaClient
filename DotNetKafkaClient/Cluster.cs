using System;
using System.Collections.Generic;
using System.Globalization;

namespace DotNetKafkaClient
{
    public class Cluster
    {
        private readonly Dictionary<int, Broker> brokers;

        /// <summary>
        /// Initializes a new instance of the <see cref="Cluster"/> class.
        /// </summary>
        public Cluster()
        {
            this.brokers = new Dictionary<int, Broker>();
        }

        //public Cluster(ZooKeeperClient zkClient) : this()
        //{
        //    var nodes = zkClient.GetChildrenParentMayNotExist(ZooKeeperClient.DefaultBrokerIdsPath);
        //    foreach (var node in nodes)
        //    {
        //        var brokerZkString = zkClient.ReadData<string>(ZooKeeperClient.DefaultBrokerIdsPath + "/" + node);
        //        Broker broker = this.CreateBroker(node, brokerZkString);
        //        brokers[broker.Id] = broker;
        //    }
        //}

        /// <summary>
        /// Initializes a new instance of the <see cref="Cluster"/> class.
        /// </summary>
        /// <param name="brokers">
        /// The set of active brokers.
        /// </param>
        public Cluster(IEnumerable<Broker> brokers) : this()
        {
            foreach (var broker in brokers)
            {
                this.brokers.Add(broker.Id, broker);
            }
        }/// <summary>
        /// Gets brokers collection
        /// </summary>
        public Dictionary<int, Broker> Brokers
        {
            get
            {
                return this.brokers;
            }
        }/// <summary>
        /// Creates a new Broker object out of a BrokerInfoString
        /// </summary>
        /// <param name="node">node string</param>
        /// <param name="brokerInfoString">the BrokerInfoString</param>
        /// <returns>Broker object</returns>
        private Broker CreateBroker(string node, string brokerInfoString)
        {
            int id;
            if (int.TryParse(node, NumberStyles.Integer, CultureInfo.InvariantCulture, out id))
            {
                return Broker.CreateBroker(id, brokerInfoString);
            }

            throw new ArgumentException(String.Format(CultureInfo.CurrentCulture, "{0} is not a valid integer", node));
        }
    }
}