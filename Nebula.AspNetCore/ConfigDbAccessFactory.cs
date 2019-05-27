using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

namespace Nebula.AspNetCore
{
    /// <summary>
    /// An access factory implementation sourced from <see cref="IConfiguration"/>.
    /// </summary>
    internal class ConfigDbAccessFactory : DocumentDbAccessFactory
    {
        private readonly NebulaConfig _config;

        /// <summary>
        /// Initialises a new instance of the <see cref="ConfigDbAccessFactory"/> class.
        /// </summary>
        /// <param name="serviceName">The service name.</param>
        /// <param name="config">The config.</param>
        internal ConfigDbAccessFactory(string serviceName, IOptions<NebulaConfig> config)
        {
            if (serviceName == null)
                throw new ArgumentNullException(nameof(serviceName));
            if (config == null)
                throw new ArgumentNullException(nameof(config));

            ServiceName = serviceName;
            _config = config.Value;
        }

        protected override string ServiceName { get; }

        protected override DocumentDbConfig GetConfig()
        {
            var builder = new DocumentDbConfigBuilder(_config.ServiceEndpoint, _config.AuthKey);

            if (_config.DatabaseId != null)
            {
                builder.SetDatabaseId(_config.DatabaseId);
            }

            if (_config.CollectionName != null)
            {
                builder.SetCollectionName(_config.CollectionName);
            }

            if (_config.DefaultRus.HasValue)
            {
                builder.SetDefaultRus(_config.DefaultRus.Value);
            }

            return builder.Finish();
        }
    }
}