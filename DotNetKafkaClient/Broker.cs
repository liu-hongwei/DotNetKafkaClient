using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Web.Script.Serialization;

namespace DotNetKafkaClient
{
    public class Broker
    {
        public const byte DefaultPortSize = 4;
        public const byte DefaultBrokerIdSize = 4;

        /// <summary>
        /// Initializes a new instance of the <see cref="Broker"/> class.
        /// </summary>
        /// <param name="id">
        /// The broker id.
        /// </param>
        /// <param name="host">
        /// The broker host.
        /// </param>
        /// <param name="port">
        /// The broker port.
        /// </param>
        public Broker(int id, string host, int port)
        {
            this.Id = id;
            this.Host = host;
            this.Port = port;
        }

        /// <summary>
        /// Gets the broker Id.
        /// </summary>
        public int Id { get;  set; }

        /// <summary>
        /// Gets the broker host.
        /// </summary>
        public string Host { get;  set; }

        /// <summary>
        /// Gets the broker port.
        /// </summary>
        public int Port { get;  set; }

        public int SizeInBytes
        {
            get
            {
                return KafkaPrimitiveTypes.GetShortStringLength(this.Host, KafkaRequest.DefaultEncoding) +
                       DefaultPortSize + DefaultBrokerIdSize;
            }
        }

        public static Broker CreateBroker(int id, string brokerInfoString)
        {
            if (string.IsNullOrEmpty(brokerInfoString))
            {
                throw new ArgumentException(string.Format("Broker id {0} does not exist", id));
            }

            var ser = new JavaScriptSerializer();
            var result = ser.Deserialize<Dictionary<string, object>>(brokerInfoString);
            var host = result["host"].ToString();
            return new Broker(id, host, int.Parse(result["port"].ToString(), CultureInfo.InvariantCulture));
        }
        public void WriteTo(MemoryStream output)
        {
            //Guard.NotNull(output, "output");

            using (var writer = new KafkaBinaryWriter(output))
            {
                this.WriteTo(writer);
            }
        }
        public void WriteTo(KafkaBinaryWriter writer)
        {
            //Guard.NotNull(writer, "writer");

            writer.Write(this.Id);
            writer.WriteShortString(this.Host, KafkaRequest.DefaultEncoding);
            writer.Write(this.Port);
        }
        internal static Broker ParseFrom(KafkaBinaryReader reader)
        {
            var id = reader.ReadInt32();
            var host = KafkaPrimitiveTypes.ReadShortString(reader, KafkaRequest.DefaultEncoding);
            var port = reader.ReadInt32();
            return new Broker(id, host, port);
        }
    }
}