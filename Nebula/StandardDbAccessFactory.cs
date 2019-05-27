using System;

namespace Nebula
{
    /// <summary>
    /// A standard access factory implementation. 
    /// </summary>
    public class StandardDbAccessFactory : DocumentDbAccessFactory
    {
        private readonly string _serviceName;
        private readonly DocumentDbConfig _config;

        /// <summary>
        /// Initialises a new instance of the <see cref="StandardDbAccessFactory"/> class.
        /// </summary>
        /// <param name="serviceName">The service name.</param>
        /// <param name="config">The database configuration.</param>
        public StandardDbAccessFactory(string serviceName, DocumentDbConfig config)
        {
            if (serviceName == null)
                throw new ArgumentNullException(nameof(serviceName));
            if (config == null)
                throw new ArgumentNullException(nameof(config));

            _serviceName = serviceName;
            _config = config;
        }

        protected override string ServiceName
        {
            get { return _serviceName; }
        }

        protected override DocumentDbConfig GetConfig()
        {
            return _config;
        }
    }
}