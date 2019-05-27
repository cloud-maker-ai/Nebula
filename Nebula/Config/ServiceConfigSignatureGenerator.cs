using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Newtonsoft.Json;

namespace Nebula.Config
{
    /// <inheritdoc />
    internal class ServiceConfigSignatureGenerator : IServiceConfigSignatureGenerator
    {
        private readonly string _serviceName;

        /// <summary>
        /// Initialises a new instance of the <see cref="ServiceConfigSignatureGenerator"/> class.
        /// </summary>
        /// <param name="serviceName">The service name.</param>
        public ServiceConfigSignatureGenerator(string serviceName)
        {
            if (serviceName == null)
                throw new ArgumentNullException(nameof(serviceName));

            _serviceName = serviceName;
        }

        /// <inheritdoc />
        public string CreateSignature(IList<DocumentStoreConfig> configs)
        {
            if (configs == null)
                throw new ArgumentNullException(nameof(configs));

            var value = new StringBuilder();

            value.Append(_serviceName);

            foreach (var storeConfig in configs.OrderBy(x => x.StoreName))
            {
                value.Append("\n");
                value.Append(storeConfig.StoreName);

                foreach (var config in storeConfig.Documents.OrderBy(x => x.DocumentName))
                {
                    // service name, store name, document name, indexes.

                    var inclusionsIdx = JsonConvert.SerializeObject(config.InclusionIndexes.OrderBy(x => x.Path));
                    var exclusionsIdx = JsonConvert.SerializeObject(config.ExclusionIndexes.OrderBy(x => x.Path));

                    value.Append("\n");
                    value.Append($";{config.DocumentName};{inclusionsIdx};{exclusionsIdx}");
                }
            }

            return CreateHash(value.ToString());
        }

        private string CreateHash(string text)
        {
            var builder = new StringBuilder();

            using (var hash = SHA256.Create())
            {
                var encoding = Encoding.UTF8;
                var result = hash.ComputeHash(encoding.GetBytes(text));

                foreach (var b in result)
                {
                    builder.Append(b.ToString("x2"));
                }
            }

            return builder.ToString();
        }
    }
}